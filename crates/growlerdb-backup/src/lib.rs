//! **Object-storage backup & restore** for a shard's index. A backup ships a shard's consistent
//! committed state (sealed Tantivy segments + the `aux.redb` aux store + the index
//! definition) to object storage; a restore pulls it onto a replacement node, which replays the tail
//! from the backed-up checkpoint. With no backup a shard is rebuilt from Iceberg — nothing is
//! irreplaceable. Transport is [`opendal`] (S3/MinIO via `s3_store`, local fs via `fs_store`).
//! Layout under a backup `prefix`:
//!
//! ```text
//! <prefix>/data/<relpath>   # each shard file's bytes (index/<segment files>, aux.redb, index.json)
//! <prefix>/manifest.json    # written LAST — its presence is the "backup complete" commit point
//! ```

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use growlerdb_core::{durable, ResolvedIndex, SourceCheckpoint};
use growlerdb_index::{ColdMarker, LocalIndexStore, Shard, ShardId};
pub use opendal::Operator;
use serde::{Deserialize, Serialize};

/// Errors from a backup or restore.
#[derive(Debug, thiserror::Error)]
pub enum BackupError {
    #[error("object store: {0}")]
    Store(#[from] opendal::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("index store: {0}")]
    Index(#[from] growlerdb_index::StoreError),
    #[error("manifest codec: {0}")]
    Codec(#[from] serde_json::Error),
    #[error("no backup found at prefix `{0}`")]
    NotFound(String),
    /// The prefix is a **bundled** cold window: its data lives in the split bundle,
    /// not per-file objects, so it can't be `restore`d — un-bundle it (`promote_cold`) instead.
    #[error("prefix `{0}` is a bundled cold window; un-bundle (promote) it rather than restore")]
    Bundled(String),
    /// A replica [`refresh`] kept racing concurrent primary backups: every bounded retry found
    /// the manifest advanced again mid-pass. Transient by nature — the caller's poll loop simply
    /// retries next tick while the previously-served shard keeps serving.
    #[error(
        "replica refresh at `{0}` kept racing concurrent primary backups — retrying next poll"
    )]
    RefreshContention(String),
    /// The manifest declares a [format](Manifest::format) newer than this binary supports: the
    /// backup was written by a newer GrowlerDB whose layout this version can't interpret, so
    /// refuse loudly rather than mis-restore.
    #[error(
        "backup manifest format {found} is newer than the supported format {supported}: this \
         backup was written by a newer GrowlerDB — restore it with a matching GrowlerDB version"
    )]
    UnsupportedFormat { found: u32, supported: u32 },
}

type Result<T> = std::result::Result<T, BackupError>;

/// The manifest **format version** this binary writes and consumes. Format **1** is the current shard
/// format; [`read_manifest`] refuses newer formats rather than mis-restore.
pub const MANIFEST_FORMAT: u32 = 1;

/// Manifests written without a `format` field deserialize as format 1.
fn default_manifest_format() -> u32 {
    1
}

/// What a backup recorded — enough to restore the shard and resume ingestion exactly-once.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Manifest **format version**: bumped on incompatible layout changes. Every consumer goes
    /// through [`read_manifest`], which refuses formats newer than [`MANIFEST_FORMAT`] with
    /// [`BackupError::UnsupportedFormat`]. Defaults to 1 when the field is absent.
    #[serde(default = "default_manifest_format")]
    pub format: u32,
    /// Index name.
    pub index: String,
    /// Shard id (its on-disk relative path component).
    pub shard: String,
    /// The committed index snapshot this backup reflects.
    pub snapshot: u64,
    /// The source checkpoint at that snapshot — a restored node resumes the tail from here.
    pub checkpoint: Option<SourceCheckpoint>,
    /// Files in the backup, relative to the shard dir (and to `<prefix>/data/`).
    pub files: Vec<FileEntry>,
    /// The resolved index definition (`index.json`), when the shard carried one.
    pub definition_json: Option<String>,
    /// Backup creation time (epoch ms).
    pub created_ms: u128,
    /// Set once a cold window has been **bundled**: the individual `index/*` data objects were
    /// removed and their bytes now live in the split bundle, so this manifest's file list no
    /// longer resolves against `<prefix>/data/`. A plain [`restore`] refuses such a prefix (it
    /// must be un-bundled — [`promote_cold`] does). Defaults to false when the field is absent.
    #[serde(default)]
    pub bundled: bool,
}

/// One backed-up file + its size (a sanity check on restore).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    pub path: String,
    pub len: u64,
}

/// Configuration for an S3/MinIO backup target.
#[derive(Debug, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>, // set for MinIO / non-AWS
    pub access_key_id: String,
    pub secret_access_key: String,
}

/// Wrap `op` with a **jittered retry layer**. Object stores return transient errors (S3 `503
/// SlowDown`, 5xx, resets) under GrowlerDB's load; without retry a single blip aborts a whole
/// backup/restore/refresh and the non-transactional file-then-manifest write can leave a partial
/// prefix. opendal retries only *temporary* errors (terminal `NotFound`/auth still surface); jitter
/// avoids a synchronized retry herd.
fn with_retry(op: Operator) -> Operator {
    op.layer(
        opendal::layers::RetryLayer::new()
            .with_max_times(4)
            .with_jitter(),
    )
}

/// An [`Operator`] over S3/MinIO. MinIO needs path-style addressing (opendal's default — virtual
/// host style stays off unless explicitly enabled). Retries transient failures.
pub fn s3_store(cfg: &S3Config) -> Result<Operator> {
    let mut b = opendal::services::S3::default()
        .bucket(&cfg.bucket)
        .region(&cfg.region);
    // Static keys only when supplied; empty ⇒ opendal's default credential chain (env / profile /
    // IMDS instance role / STS assume-role / IRSA web-identity), so IAM-based auth works — D56.
    if !cfg.access_key_id.is_empty() && !cfg.secret_access_key.is_empty() {
        b = b
            .access_key_id(&cfg.access_key_id)
            .secret_access_key(&cfg.secret_access_key);
    }
    if let Some(ep) = &cfg.endpoint {
        b = b.endpoint(ep);
    }
    Ok(with_retry(Operator::new(b)?))
}

/// Hidden directory (under the fs store root) where [`fs_store`] stages writes before the atomic
/// rename into place. Never collides with GrowlerDB object keys — every store listing/deletion is
/// prefix-scoped (`backups/…`, `cold/…`), never the bare root.
pub const FS_ATOMIC_WRITE_DIR: &str = ".atomic-writes";

/// An [`Operator`] over a local directory — a filesystem backup target (mounted volume / NFS),
/// and the backend the tests use. Retries transient failures.
///
/// **Writes are atomic** (HA-G4): `atomic_write_dir` stages every write in a tempfile under
/// [`FS_ATOMIC_WRITE_DIR`] and renames it into place on close, so a concurrent reader (a replica
/// fetching `cold.json` / `manifest.json` mid-overwrite) never sees a torn object. POSIX rename is
/// atomic within the one filesystem the staging dir shares. S3 backends need none of this (a PUT is
/// atomic); a crash can leave a harmless stale tempfile outside every listed prefix.
pub fn fs_store(root: impl AsRef<Path>) -> Result<Operator> {
    let root = root.as_ref();
    std::fs::create_dir_all(root)?;
    let atomic = root.join(FS_ATOMIC_WRITE_DIR);
    std::fs::create_dir_all(&atomic)?;
    let b = opendal::services::Fs::default()
        .root(&root.to_string_lossy())
        .atomic_write_dir(&atomic.to_string_lossy());
    Ok(with_retry(Operator::new(b)?))
}

/// Back up `shard` (named `index`/`shard`) to `store` under `prefix`. `staging` is a scratch dir —
/// for instant segment hard-links it should sit on the **same filesystem** as the shard. The
/// index `definition_json` (the index-root `index.json`, which is *not* a shard file) is recorded
/// in the manifest so a restore can re-materialize the definition. The manifest is written last,
/// so a crashed backup never looks complete.
#[allow(clippy::too_many_arguments)]
pub async fn backup(
    shard: &Shard,
    index: &str,
    shard_id: &str,
    staging: &Path,
    store: &Operator,
    prefix: &str,
    definition_json: Option<String>,
) -> Result<Manifest> {
    if staging.exists() {
        std::fs::remove_dir_all(staging)?;
    }
    std::fs::create_dir_all(staging)?;

    // Consistent committed snapshot of the shard's files (under the writer lock).
    let snap = shard.backup_snapshot(staging)?;

    let prefix = prefix.trim_end_matches('/');
    let mut entries = Vec::with_capacity(snap.files.len());
    for rel in &snap.files {
        let bytes = std::fs::read(staging.join(rel))?;
        let rel_str = rel.to_string_lossy().replace('\\', "/");
        store
            .write(&format!("{prefix}/data/{rel_str}"), bytes.clone())
            .await?;
        entries.push(FileEntry {
            path: rel_str,
            len: bytes.len() as u64,
        });
    }

    let manifest = Manifest {
        format: MANIFEST_FORMAT,
        index: index.to_string(),
        shard: shard_id.to_string(),
        snapshot: snap.snapshot,
        checkpoint: snap.checkpoint,
        files: entries,
        definition_json,
        created_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0),
        bundled: false,
    };
    // Read the manifest we're about to replace — its file set and source snapshot drive the
    // snapshot-aware GC below. Absent (Err) on the first backup to this prefix.
    let previous = read_manifest(store, prefix).await.ok();

    // Written LAST — its presence is the "backup is complete and restorable" commit point.
    store
        .write(
            &format!("{prefix}/manifest.json"),
            serde_json::to_vec(&manifest)?,
        )
        .await?;

    // Snapshot-aware cold GC. A cold read-through reader (D53) reopens only when the SOURCE SNAPSHOT
    // advances, so deleting objects a same-snapshot re-layout (finalize-merge / compaction)
    // superseded would 404 a reader still pinned to the old layout. So: same snapshot → retain ALL
    // superseded objects; snapshot ADVANCE → prune, but retain the previous snapshot's files one
    // generation for a replica still mid-reopen. Run AFTER the manifest commit (a crash here leaves
    // reclaimable orphans a later advancing GC reclaims). Bounded: same-snapshot superseded objects
    // accumulate only until the next source commit.
    if let Some(prev) = &previous {
        if manifest.snapshot > prev.snapshot {
            prune_superseded(store, prefix, &manifest, Some(prev)).await?;
        }
    }

    let _ = std::fs::remove_dir_all(staging);
    Ok(manifest)
}

/// List every **object** key under `prefix` (recursive), filtering out the trailing-slash directory
/// markers the fs backend emits. The shared scan behind prune / bundle-delete / promote.
async fn list_object_keys(store: &Operator, prefix: &str) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    for entry in store.list_with(prefix).recursive(true).await? {
        let key = entry.path();
        if !key.ends_with('/') {
            keys.push(key.to_string());
        }
    }
    Ok(keys)
}

/// Best-effort delete every object under `prefix` (recursive), swallowing list/per-key errors — for
/// reclaiming a superseded prefix where a straggler is harmless. Callers needing a
/// count or hard failure use [`list_object_keys`] directly.
async fn delete_prefix_best_effort(store: &Operator, prefix: &str) {
    if let Ok(keys) = list_object_keys(store, prefix).await {
        for key in keys {
            let _ = store.delete(&key).await;
        }
    }
}

/// Delete objects under `{prefix}/data/` referenced by neither the just-committed `manifest` nor
/// `retain_previous` (the manifest this one replaces). Keeping the previous generation's files is a
/// one-generation grace: a replica still mid-reopen from the prior snapshot can finish its in-flight
/// lazy fetches before the objects vanish. Idempotent; returns the number of objects pruned.
///
/// **Precondition — single writer per prefix** (shard ownership): concurrent `backup()`s to one
/// prefix could have this prune delete a file the other just committed. A replica racing on an older
/// manifest is caught by [`refresh`]'s re-read-and-retry on a mid-flight `NotFound`.
async fn prune_superseded(
    store: &Operator,
    prefix: &str,
    manifest: &Manifest,
    retain_previous: Option<&Manifest>,
) -> Result<usize> {
    let data_prefix = format!("{prefix}/data/");
    let mut wanted: std::collections::HashSet<&str> =
        manifest.files.iter().map(|f| f.path.as_str()).collect();
    if let Some(prev) = retain_previous {
        wanted.extend(prev.files.iter().map(|f| f.path.as_str()));
    }
    let mut pruned = 0;
    // Recursive: segment files live directly under data/ but travel through an `index/` subdir.
    for key in list_object_keys(store, &data_prefix).await? {
        if let Some(rel) = key.strip_prefix(&data_prefix) {
            if !wanted.contains(rel) {
                store.delete(&key).await?;
                pruned += 1;
            }
        }
    }
    Ok(pruned)
}

/// Park a **cold** shard for tiered storage: back it up to `store` under `prefix`, then —
/// only once the manifest is committed, so the backup is restorable — drop the open shard and evict
/// its local directory `shard_dir`. The shard is taken **by value** so its file handles (redb +
/// tantivy mmaps) close before the directory is unlinked. A parked window then lives only in object
/// storage, freeing hot NVMe, until [`revive`] restores it.
#[allow(clippy::too_many_arguments)]
pub async fn park(
    shard: Shard,
    index: &str,
    shard_id: &str,
    shard_dir: &Path,
    staging: &Path,
    store: &Operator,
    prefix: &str,
    definition_json: Option<String>,
) -> Result<Manifest> {
    let manifest = backup(
        &shard,
        index,
        shard_id,
        staging,
        store,
        prefix,
        definition_json,
    )
    .await?;
    // Backup committed (manifest written last) → safe to drop local state. Close all handles
    // before unlinking so nothing writes into a half-removed directory.
    drop(shard);
    std::fs::remove_dir_all(shard_dir)?;
    Ok(manifest)
}

/// Revive a parked shard: restore the backup at `prefix` back into `shard_dir` — the
/// inverse of [`park`]. A thin wrapper over [`restore`] named for the cold-tiering lifecycle; the
/// caller then opens the shard and ingestion replays the tail from the manifest checkpoint.
pub async fn revive(store: &Operator, prefix: &str, shard_dir: &Path) -> Result<Manifest> {
    restore(store, prefix, shard_dir).await
}

/// Evict a parked window's local Tantivy **bulk** (`window_dir/index`) while keeping the local
/// `aux.redb` (the cold footprint `open_cold_shard` still reads). The LAST step of a
/// park — run only *after* the [`ColdMarker`] is durable, so a crash mid-park always leaves a
/// fully-serving hot shard, never a markerless empty window.
pub fn evict_local_index(window_dir: &Path) -> std::io::Result<()> {
    let index_subdir = window_dir.join("index");
    if index_subdir.exists() {
        std::fs::remove_dir_all(&index_subdir)?;
    }
    Ok(())
}

/// The **cold-park core** (borrows the shard, does NOT evict): back the window's bulk up to `store`
/// under `prefix`, build the precomputed hotcache + split bundle, and drop a durable [`ColdMarker`]
/// in `window_dir`. Returns the marker. Eviction of the local `index/` bulk is the caller's step
/// (via [`evict_local_index`]) — split out so a live node can park a window it is *serving* (backing
/// up through its shared read handle) without a second writer on the index directory, then swap the
/// handle to a read-through shard before evicting. Both [`cold_park`] and [`cold_park_in_place`] wrap
/// this.
#[allow(clippy::too_many_arguments)]
async fn cold_park_to_store(
    shard: &Shard,
    index: &str,
    window: i64,
    window_dir: &Path,
    staging: &Path,
    store: &Operator,
    prefix: &str,
    definition_json: Option<String>,
) -> Result<ColdMarker> {
    // The event-time zone-map travels into the marker so the gateway can prune a cold window
    // without opening it.
    let zone = shard.event_bounds()?;
    let mut manifest = backup(
        shard,
        index,
        &format!("w{window}"),
        staging,
        store,
        prefix,
        definition_json,
    )
    .await?;
    let base = prefix.trim_end_matches('/');
    let object_prefix = format!("{base}/data/index");
    // Precomputed hotcache: warm the just-parked index once and store the structural reads
    // as a sidecar, so cold opens issue zero object round-trips. Kept OUTSIDE `{prefix}/data/` so the
    // backup GC (which prunes unreferenced data objects) never touches it. Best-effort: a failure to
    // build it just means cold opens fall back to plain read-through, so don't fail the park.
    let hotcache_key = {
        let op = store.clone();
        let op_prefix = object_prefix.clone();
        let built =
            tokio::task::spawn_blocking(move || growlerdb_index::hotcache::build(op, &op_prefix))
                .await
                .ok()
                .and_then(|r| r.ok());
        match built {
            Some(bytes) => {
                let key = format!("{base}/hotcache.bin");
                store.write(&key, bytes).await?;
                Some(key)
            }
            None => None,
        }
    };
    // Split bundle: concatenate the parked index files into ONE object so cold queries issue ranged
    // GETs against a single object instead of one per file. On success the now-redundant individual
    // index objects are removed (the bundle is the sole serving copy — no storage doubling); on
    // failure keep them and fall back to per-file read-through. Stored OUTSIDE `data/` so backup GC
    // won't touch it, and built AFTER the hotcache (which reads the individual files). Bundled from
    // the LOCAL window files (still on disk pre-eviction), so `index/` manifest entries stripped of
    // that prefix are the bare rels, read from `window_dir/index`.
    let index_rels: Vec<String> = manifest
        .files
        .iter()
        .filter_map(|f| f.path.strip_prefix("index/").map(str::to_string))
        .collect();
    let local_index_dir = window_dir.join("index");
    let (bundle_key, bundle_manifest_key) = {
        let bkey = format!("{base}/split.bundle");
        let mkey = format!("{base}/split.manifest");
        match growlerdb_index::bundle::build_from_dir(
            store,
            &local_index_dir,
            &index_rels,
            &bkey,
            &mkey,
        )
        .await
        {
            Ok(_) => {
                // Commit the `bundled` manifest BEFORE deleting the per-file objects, so every crash
                // point stays consistent: rewrite fails ⇒ objects kept and the old manifest still
                // restores; rewrite lands ⇒ the objects are unreferenced and their deletion is pure
                // (best-effort) reclamation.
                manifest.bundled = true;
                manifest.files.retain(|f| !f.path.starts_with("index/"));
                let manifest_committed = match serde_json::to_vec(&manifest) {
                    Ok(bytes) => store
                        .write(&format!("{base}/manifest.json"), bytes)
                        .await
                        .is_ok(),
                    Err(_) => false,
                };
                if manifest_committed {
                    delete_prefix_best_effort(store, &format!("{object_prefix}/")).await;
                }
                (Some(bkey), Some(mkey))
            }
            Err(_) => (None, None),
        }
    };
    // The aux sidecars `backup()` already uploaded to `{base}/data/` (they survive the index-only
    // bundling above and the manifest keeps them): recorded here so a **replica** on another node can
    // fetch them and open the window read-through (D53). A parked window is immutable, so this is a
    // one-time, frozen snapshot — no continuous re-sync.
    let marker = ColdMarker {
        object_prefix,
        event_min: zone.map(|(lo, _)| lo),
        event_max: zone.map(|(_, hi)| hi),
        snapshot: manifest.snapshot,
        hotcache_key,
        bundle_key,
        bundle_manifest_key,
        aux_key: Some(format!("{base}/data/aux.redb")),
    };
    std::fs::write(
        window_dir.join(growlerdb_index::COLD_MARKER),
        serde_json::to_vec_pretty(&marker)?,
    )?;
    // Also publish the marker to object storage (`{prefix}/cold.json`) so a **replica** on another
    // node — which has no local window dir — can fetch it and open the window read-through (D53,
    // [`fetch_cold_marker`] → [`open_cold_replica`](growlerdb_index::LocalIndexStore::open_cold_replica)).
    store
        .write(
            &format!("{base}/{}", growlerdb_index::COLD_MARKER),
            serde_json::to_vec(&marker)?,
        )
        .await?;
    Ok(marker)
}

/// Back up a **hot, writable** shard to `store` under `prefix` and publish a replica-ready
/// [`ColdMarker`] to `{prefix}/cold.json` — **without evicting the local copy**, so a cross-node
/// replica can open the shard read-through ([`open_cold_replica`](growlerdb_index::LocalIndexStore::open_cold_replica))
/// while the primary keeps serving and writing it (D53 hash-shard parity).
///
/// Unlike [`cold_park`] — which parks an *aged, immutable* window (evict → read-through) — a **hash
/// ordinal** never parks: it stays hot and writable on its primary. So this is a **frozen snapshot** of
/// a live shard, and it trails the primary's later writes until the next backup (immutable-first;
/// continuous hot-shard shipping is deferred). It carries no event zone-map (ordinals aren't time
/// windows) and skips the hotcache/bundle sidecars (open falls back to plain per-file read-through).
/// Returns the published marker.
pub async fn backup_replica_snapshot(
    shard: &Shard,
    index: &str,
    shard_id: &str,
    staging: &Path,
    store: &Operator,
    prefix: &str,
    definition_json: Option<String>,
) -> Result<ColdMarker> {
    let manifest = backup(
        shard,
        index,
        shard_id,
        staging,
        store,
        prefix,
        definition_json,
    )
    .await?;
    let base = prefix.trim_end_matches('/');
    // The aux + location sidecars `backup()` uploaded to `{base}/data/` (in the manifest's file set),
    // recorded so a replica fetches them and opens the shard read-through — the same keys the windowed
    // park marker records.
    let marker = ColdMarker {
        object_prefix: format!("{base}/data/index"),
        event_min: None,
        event_max: None,
        snapshot: manifest.snapshot,
        hotcache_key: None,
        bundle_key: None,
        bundle_manifest_key: None,
        aux_key: Some(format!("{base}/data/aux.redb")),
    };
    store
        .write(
            &format!("{base}/{}", growlerdb_index::COLD_MARKER),
            serde_json::to_vec(&marker)?,
        )
        .await?;
    Ok(marker)
}

/// Fetch a parked window's [`ColdMarker`] from object storage — the `{prefix}/cold.json` that
/// [`cold_park`]/[`cold_park_in_place`] (or [`backup_replica_snapshot`]) published — so a **replica**
/// node can open the unit read-through without a local copy (D53). `prefix` is the unit's park prefix
/// (`cold/{index}/w{window}` for a window, `cold/{index}/{ordinal}` for a hash shard). `Ok(None)` if
/// the unit isn't published (no marker object).
pub async fn fetch_cold_marker(store: &Operator, prefix: &str) -> Result<Option<ColdMarker>> {
    let base = prefix.trim_end_matches('/');
    let key = format!("{base}/{}", growlerdb_index::COLD_MARKER);
    match store.read(&key).await {
        Ok(buf) => Ok(Some(serde_json::from_slice(&buf.to_vec())?)),
        Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// **Cold-park** a window shard for *read-through* serving: back its bulk up to `store`
/// under `prefix`, then evict only the local Tantivy `index/` dir while **keeping `aux.redb`**, and
/// drop a [`ColdMarker`] in `window_dir`. Unlike [`park`] (full evict → unqueryable until restored),
/// the window stays **searchable in place** — `open_cold_shard` serves the index read-through from
/// `<prefix>/data/index` with the local aux. Returns the marker. The shard is **consumed** so its
/// handles (redb + tantivy) close before the `index/` dir is removed — the offline CLI path, where
/// nothing else is serving the window.
#[allow(clippy::too_many_arguments)]
pub async fn cold_park(
    shard: Shard,
    index: &str,
    window: i64,
    window_dir: &Path,
    staging: &Path,
    store: &Operator,
    prefix: &str,
    definition_json: Option<String>,
) -> Result<ColdMarker> {
    let marker = cold_park_to_store(
        &shard,
        index,
        window,
        window_dir,
        staging,
        store,
        prefix,
        definition_json,
    )
    .await?;
    // Backup + marker durable → close handles before touching the directory, then evict the local
    // bulk LAST: a crash before the marker leaves a fully-serving hot shard; after it, discovery
    // serves the window cold read-through.
    drop(shard);
    evict_local_index(window_dir)?;
    Ok(marker)
}

/// **Cold-park a window a live node is serving**, backing up through a shared read handle to the
/// shard (`&Shard`, no second writer). Returns the [`ColdMarker`]; the caller must then swap the
/// window's handle to a read-through shard ([`open_cold_shard`](growlerdb_index::LocalIndexStore::open_cold_shard))
/// and call [`evict_local_index`] — in that order, so queries never see a gap (the hot shard serves
/// until the swap; the read-through shard reads object storage + the still-local `aux.redb`, so
/// evicting the local `index/` after the swap is safe). The marker is durable before this returns.
#[allow(clippy::too_many_arguments)]
pub async fn cold_park_in_place(
    shard: &Shard,
    index: &str,
    window: i64,
    window_dir: &Path,
    staging: &Path,
    store: &Operator,
    prefix: &str,
    definition_json: Option<String>,
) -> Result<ColdMarker> {
    cold_park_to_store(
        shard,
        index,
        window,
        window_dir,
        staging,
        store,
        prefix,
        definition_json,
    )
    .await
}

/// Promote a cold (read-through) window back to a **local hot shard**: materialize
/// its Tantivy index files locally under `window_dir/index` — from the split bundle when present, else
/// the individual objects (unbundled windows) — then drop the `cold.json` marker. The window's
/// `aux.redb` is already local, so afterward `open_shard` opens a normal on-NVMe hot shard with no
/// cold latency; the caller swaps it into the live handle. On success the window's now-unused
/// object-storage copies (bundle / hotcache / backup) are reclaimed, which also
/// mops up any `data/index/*` orphaned by a crashed bundle-delete.
pub async fn promote_cold(store: &Operator, marker: &ColdMarker, window_dir: &Path) -> Result<()> {
    let index_dir = window_dir.join("index");
    std::fs::create_dir_all(&index_dir)?;
    match (
        marker.bundle_key.as_deref(),
        marker.bundle_manifest_key.as_deref(),
    ) {
        (Some(bundle_key), Some(manifest_key)) => {
            growlerdb_index::bundle::unbundle(store, bundle_key, manifest_key, &index_dir).await?;
        }
        _ => {
            // Unbundled cold window: pull the individual index objects down.
            let base = format!("{}/", marker.object_prefix.trim_end_matches('/'));
            for key in list_object_keys(store, &base).await? {
                let rel = key.strip_prefix(base.as_str()).unwrap_or(key.as_str());
                let bytes = store.read(&key).await?.to_vec();
                let dst = index_dir.join(rel);
                if let Some(parent) = dst.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                durable::write(&dst, &bytes)?;
            }
        }
    }
    durable::sync_dir(&index_dir)?;
    // Drop the cold marker → discovery/open now treats this as a hot local window.
    let _ = std::fs::remove_file(window_dir.join(growlerdb_index::COLD_MARKER));
    durable::sync_dir(window_dir)?;
    // Reclaim the window's object-storage copies now that it's served locally:
    // remove everything under the window's backup prefix — bundle, split.manifest, hotcache.bin,
    // data/, manifest.json. Best-effort: a failure just leaves reclaimable objects, never breaks the
    // now-local shard. `object_prefix` is `<prefix>/data/index`, so strip that to get the prefix root.
    if let Some(base) = marker.object_prefix.strip_suffix("/data/index") {
        let _ = store.delete_with(base).recursive(true).await;
    }
    Ok(())
}

/// Read a backup's manifest from `store` under `prefix` (without downloading the data). The single
/// funnel every manifest consumer uses (restore / revive / refresh / status), so this is where a
/// manifest [format](Manifest::format) newer than [`MANIFEST_FORMAT`] is refused: a newer layout
/// can't be interpreted here, and failing loudly beats mis-restoring.
pub async fn read_manifest(store: &Operator, prefix: &str) -> Result<Manifest> {
    let prefix = prefix.trim_end_matches('/');
    let key = format!("{prefix}/manifest.json");
    match store.read(&key).await {
        Ok(buf) => {
            let manifest: Manifest = serde_json::from_slice(&buf.to_vec())?;
            if manifest.format > MANIFEST_FORMAT {
                return Err(BackupError::UnsupportedFormat {
                    found: manifest.format,
                    supported: MANIFEST_FORMAT,
                });
            }
            Ok(manifest)
        }
        Err(e) if e.kind() == opendal::ErrorKind::NotFound => {
            Err(BackupError::NotFound(prefix.to_string()))
        }
        Err(e) => Err(e.into()),
    }
}

/// Restore the backup at `prefix` into `dest` (the shard directory on the replacement node).
/// Returns the manifest; the caller opens the shard and lets ingestion replay the tail from
/// `manifest.checkpoint`. Errors with [`BackupError::NotFound`] when there is no backup — the
/// caller's cue to rebuild from Iceberg instead.
pub async fn restore(store: &Operator, prefix: &str, dest: &Path) -> Result<Manifest> {
    let manifest = read_manifest(store, prefix).await?;
    // A bundled cold-window prefix has no `index/*` data objects (they live in the split bundle), so
    // a per-file restore can't rebuild it — refuse cleanly rather than 404 mid-download. Such a
    // window is un-bundled by `promote_cold`, not restored.
    if manifest.bundled {
        return Err(BackupError::Bundled(prefix.to_string()));
    }
    let prefix = prefix.trim_end_matches('/');
    std::fs::create_dir_all(dest)?;
    for entry in &manifest.files {
        let buf = store.read(&format!("{prefix}/data/{}", entry.path)).await?;
        let bytes = buf.to_vec();
        let dst = dest.join(&entry.path);
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }
        durable::write(&dst, &bytes)?;
    }
    durable::sync_dir(dest)?;
    if dest.join("index").exists() {
        durable::sync_dir(&dest.join("index"))?;
    }
    Ok(manifest)
}

/// What a replica [`refresh`] transferred.
#[derive(Debug, Clone)]
pub struct RefreshStats {
    /// The manifest the replica is now at.
    pub manifest: Manifest,
    /// Files fetched this refresh (new segments + the mutable meta/locator).
    pub downloaded: usize,
    /// Immutable segment files already present and reused — the "ship only new segments" win.
    pub skipped: usize,
    /// Stale local index files removed (segments compacted away on the primary).
    pub removed: usize,
}

/// Refresh a **replica** shard at `dest` from the primary's backup at `prefix` — segment
/// shipping: the replica *pulls sealed segments* rather than re-indexing the source. Incremental:
/// immutable segment files already present (same path + size) are skipped; the mutable
/// `meta.json` / `.managed.json` / `aux.redb` are always re-fetched; and local index files no
/// longer in the manifest (compacted away on the primary) are pruned. Because segments are copied
/// **byte-for-byte**, a replica scores identically to the primary. The caller (re)opens the shard
/// afterward; the first refresh of an empty `dest` downloads everything.
pub async fn refresh(store: &Operator, prefix: &str, dest: &Path) -> Result<RefreshStats> {
    // Bounded retries over the two ways a concurrent primary backup can race this pass:
    //
    // * A listed segment **404s** mid-download — the backup's GC (`prune_superseded`) pruned a
    //   file this now-stale manifest still names. Re-read and go again.
    // * The pass **tears**: the mutable objects (`index/meta.json`, `aux.redb`)
    //   are fetched live while segments come from the manifest's list, so a backup landing mid-pass
    //   can pair a NEWER meta with the OLDER segment set. The manifest is the commit point (written
    //   last), so re-reading it after the pass and comparing snapshots detects any backup that
    //   completed during it; the retry is cheap (already-downloaded immutable segments are reused).
    //   A sub-object read race narrower than the manifest commit is bounded by one GET, not the pass.
    const MAX_REFRESH_RETRIES: usize = 3;
    let mut manifest = read_manifest(store, prefix).await?;
    // One re-read covers the GC race; a SECOND NotFound is a genuinely missing object and
    // surfaces as the store error (unbounded 404 retries would mask real corruption).
    let mut retried_404 = false;
    for _ in 0..=MAX_REFRESH_RETRIES {
        match refresh_once(store, prefix, dest, manifest).await {
            Ok(stats) => {
                let current = read_manifest(store, prefix).await?;
                if current.snapshot == stats.manifest.snapshot {
                    return Ok(stats);
                }
                manifest = current; // torn: a backup completed mid-pass — refresh against it
            }
            Err(BackupError::Store(e))
                if e.kind() == opendal::ErrorKind::NotFound && !retried_404 =>
            {
                retried_404 = true;
                manifest = read_manifest(store, prefix).await?;
            }
            Err(e) => return Err(e),
        }
    }
    Err(BackupError::RefreshContention(prefix.to_string()))
}

async fn refresh_once(
    store: &Operator,
    prefix: &str,
    dest: &Path,
    manifest: Manifest,
) -> Result<RefreshStats> {
    let prefix = prefix.trim_end_matches('/');
    let index_dir = dest.join("index");
    std::fs::create_dir_all(&index_dir)?;

    let mut downloaded = 0;
    let mut skipped = 0;
    let wanted: std::collections::HashSet<&str> =
        manifest.files.iter().map(|f| f.path.as_str()).collect();
    for entry in &manifest.files {
        let dst = dest.join(&entry.path);
        // The index meta + aux store change every commit; segment files are immutable.
        let mutable = matches!(
            entry.path.as_str(),
            "aux.redb" | "index/meta.json" | "index/.managed.json"
        );
        if !mutable
            && dst
                .metadata()
                .map(|m| m.len() == entry.len)
                .unwrap_or(false)
        {
            skipped += 1;
            continue;
        }
        let buf = store.read(&format!("{prefix}/data/{}", entry.path)).await?;
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }
        durable::write(&dst, &buf.to_vec())?;
        downloaded += 1;
    }

    // Prune segments compacted away on the primary (local index/ files not in the manifest). Safe:
    // the caller reopens after refresh, and unlinking a still-mmapped file is fine on Unix.
    let mut removed = 0;
    for de in std::fs::read_dir(&index_dir)? {
        let de = de?;
        if !de.file_type()?.is_file() {
            continue;
        }
        let rel = format!("index/{}", de.file_name().to_string_lossy());
        if !wanted.contains(rel.as_str()) {
            std::fs::remove_file(de.path())?;
            removed += 1;
        }
    }
    durable::sync_dir(dest)?;
    durable::sync_dir(&index_dir)?;
    Ok(RefreshStats {
        manifest,
        downloaded,
        skipped,
        removed,
    })
}

/// One **live read-replica** refresh cycle: [`refresh`] the replica's `shard_id` shard in `store`
/// and re-open it **only when the primary has moved on** — returning the fresh shard for the caller
/// to hot-swap (e.g. `ShardHandle::swap`). The signal is the backup's **snapshot** advancing past
/// `served_snapshot`: the mutable `meta.json`/`aux.redb` re-download every poll and opening writes a
/// local writer-lock the next refresh prunes, so the raw `RefreshStats` counts can't tell idle from
/// changed. A same-snapshot compaction leaves query results unchanged, so skipping its re-open is
/// correct. On a snapshot advance the definition is re-materialized at `def_path` (when the manifest
/// carries one and `def_path` is set) so the replica tracks the primary's schema, and the shard is
/// re-opened; `Ok((None, stats))` means already up to date. The swap is the caller's concern, keeping
/// this pure of the server loop and unit-testable against an `fs` backup.
pub async fn refresh_and_reopen(
    store: &Operator,
    prefix: &str,
    out_store: &LocalIndexStore,
    shard_id: &ShardId,
    resolved: &ResolvedIndex,
    def_path: Option<&Path>,
    served_snapshot: u64,
) -> Result<(Option<Shard>, RefreshStats)> {
    let dest = out_store.shard_path(shard_id);
    let stats = refresh(store, prefix, &dest).await?;
    if stats.manifest.snapshot == served_snapshot {
        // Same snapshot ⇒ the replica already serves the primary's data; skip the re-open.
        return Ok((None, stats));
    }
    if let (Some(path), Some(def)) = (def_path, &stats.manifest.definition_json) {
        durable::write(path, def.as_bytes())?;
    }
    let shard = out_store.open_shard(shard_id, resolved)?;
    Ok((Some(shard), stats))
}

#[cfg(test)]
mod tests {
    use super::*;
    use growlerdb_core::{
        CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc, Query,
        SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
    };
    use growlerdb_index::LocalIndexStore;
    use std::collections::BTreeMap;

    fn docs_index() -> growlerdb_core::ResolvedIndex {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("body", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: body, type: TEXT } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap()
    }

    fn doc(id: &str) -> LocatedDoc {
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from(id));
        f.insert("body".to_string(), Value::from("text"));
        LocatedDoc {
            doc: Document::new(key, f),
        }
    }

    /// HA-G4: the local-fs object store must never overwrite an object **in place** — a replica
    /// reading `cold.json`/`manifest.json` while a re-park rewrites it would see a torn object.
    /// [`fs_store`] stages every write in the hidden [`FS_ATOMIC_WRITE_DIR`] and renames it into
    /// place: proven here by the object's **inode changing** across an overwrite (an in-place write
    /// keeps the inode; a rename swaps in a new file), with no tempfile leak and no staging-dir
    /// pollution of prefix-scoped listings.
    #[tokio::test]
    async fn fs_store_overwrites_are_atomic_renames_not_in_place_writes() {
        let tmp = tempfile::tempdir().unwrap();
        let op = fs_store(tmp.path()).unwrap();
        let key = "cold/logs/w1/cold.json";
        op.write(key, vec![b'a'; 4096]).await.unwrap();
        let on_disk = tmp.path().join(key);
        #[cfg(unix)]
        let ino_before = {
            use std::os::unix::fs::MetadataExt;
            std::fs::metadata(&on_disk).unwrap().ino()
        };
        op.write(key, vec![b'b'; 8192]).await.unwrap();
        assert_eq!(
            op.read(key).await.unwrap().to_vec(),
            vec![b'b'; 8192],
            "the overwrite is fully visible"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_ne!(
                ino_before,
                std::fs::metadata(&on_disk).unwrap().ino(),
                "an overwrite must REPLACE the object via rename (new inode), never patch the \
                 bytes a concurrent reader may hold open"
            );
        }
        // Completed writes drain their tempfiles, and the staging dir stays invisible to the
        // prefix-scoped listings every consumer uses.
        let staging = tmp.path().join(FS_ATOMIC_WRITE_DIR);
        assert!(staging.is_dir());
        assert_eq!(
            std::fs::read_dir(&staging).unwrap().count(),
            0,
            "no tempfile leaks after completed writes"
        );
        assert_eq!(
            list_object_keys(&op, "cold/").await.unwrap(),
            vec![key.to_string()]
        );
    }

    /// The torn-refresh hazard and its guard: a pass against a **stale** manifest (while the store
    /// already holds a newer backup) pairs live mutable objects with the old segment list, assembling
    /// a shard whose meta references segments it never downloaded. `refresh_once` reproduces that; the
    /// public [`refresh`] re-reads the manifest after the pass and retries, converging on an openable
    /// shard.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stale_manifest_pass_tears_and_refresh_converges() {
        let primary_tmp = tempfile::tempdir().unwrap();
        let store_tmp = tempfile::tempdir().unwrap();
        let replica_tmp = tempfile::tempdir().unwrap();
        let staging = primary_tmp.path().join(".staging");
        let op = fs_store(store_tmp.path()).unwrap();
        let idx = docs_index();
        let primary_store = LocalIndexStore::open(primary_tmp.path()).unwrap();
        let shard = primary_store
            .create_shard(&growlerdb_index::ShardId::single("docs"), &idx)
            .unwrap();

        // Backup v1 (doc a), keep its manifest — the stale one.
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(vec![doc("a")], SourceCheckpoint::iceberg(1), "b1"),
        )
        .unwrap();
        backup(&shard, "docs", "docs", &staging, &op, "backups/docs", None)
            .await
            .unwrap();
        let stale = read_manifest(&op, "backups/docs").await.unwrap();

        // Backup v2 (doc b) — the store's mutable objects now belong to v2.
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(vec![doc("b")], SourceCheckpoint::iceberg(2), "b2"),
        )
        .unwrap();
        backup(&shard, "docs", "docs", &staging, &op, "backups/docs", None)
            .await
            .unwrap();

        // The raw pass against the stale manifest = a backup landing mid-pass: v2 meta/aux paired
        // with v1's segment list. The assembled dir must not open as a working shard.
        let replica = LocalIndexStore::open(replica_tmp.path()).unwrap();
        let dest = replica.shard_path(&growlerdb_index::ShardId::single("docs"));
        refresh_once(&op, "backups/docs", &dest, stale)
            .await
            .unwrap();
        let torn = replica.open_shard(&growlerdb_index::ShardId::single("docs"), &idx);
        assert!(
            torn.is_err(),
            "a torn refresh (new meta, old segment set) must not open cleanly"
        );

        // The guarded public refresh converges: consistent manifest, shard opens, both docs.
        let stats = refresh(&op, "backups/docs", &dest).await.unwrap();
        assert_eq!(stats.manifest.snapshot, 2);
        let healed = replica
            .open_shard(&growlerdb_index::ShardId::single("docs"), &idx)
            .expect("guarded refresh assembles a consistent shard");
        for id in ["a", "b"] {
            assert_eq!(
                healed
                    .search_all(&Query::parse(&format!("id:{id}")).unwrap(), 10)
                    .unwrap()
                    .len(),
                1,
                "doc {id} present after the guarded refresh"
            );
        }
    }

    /// Segment file paths (`index/*`) in a manifest — the objects a reader lazily fetches.
    fn segment_files(m: &Manifest) -> std::collections::HashSet<String> {
        m.files
            .iter()
            .map(|f| f.path.clone())
            .filter(|p| p.starts_with("index/"))
            .collect()
    }

    async fn object_exists(op: &Operator, prefix: &str, rel: &str) -> bool {
        op.stat(&format!("{prefix}/data/{rel}")).await.is_ok()
    }

    /// Snapshot-gated cold GC: a same-snapshot re-layout (finalize-merge / compaction) must RETAIN
    /// the superseded objects, since a cold read-through reader pinned to the old layout reopens only
    /// on a SOURCE SNAPSHOT advance and would 404 on its next lazy fetch. A snapshot advance (past all
    /// such readers) reclaims them — not an unbounded leak.
    #[tokio::test]
    async fn same_snapshot_compaction_retains_superseded_cold_objects() {
        let primary_tmp = tempfile::tempdir().unwrap();
        let store_tmp = tempfile::tempdir().unwrap();
        let staging = primary_tmp.path().join(".staging");
        let op = fs_store(store_tmp.path()).unwrap();
        let idx = docs_index();
        let store = LocalIndexStore::open(primary_tmp.path()).unwrap();
        let shard = store
            .create_shard(&growlerdb_index::ShardId::single("docs"), &idx)
            .unwrap();
        let prefix = "backups/docs";

        // Two commits → two segments, at source snapshot 2.
        for (id, cp) in [("a", 1u64), ("b", 2)] {
            IndexWriter::write(
                &shard,
                &CommitBatch::from_upserts(vec![doc(id)], SourceCheckpoint::iceberg(cp as i64), id),
            )
            .unwrap();
        }
        let pre = backup(&shard, "docs", "docs", &staging, &op, prefix, None)
            .await
            .unwrap();
        assert_eq!(pre.snapshot, 2);
        let pre_files = segment_files(&pre);

        // Compact — fuses the two segments into a new, differently-named one. The source snapshot is
        // unchanged (no new commit), so this is a same-snapshot re-layout.
        shard
            .compact(&growlerdb_index::CompactionPolicy::default())
            .unwrap();
        let post = backup(&shard, "docs", "docs", &staging, &op, prefix, None)
            .await
            .unwrap();
        assert_eq!(
            post.snapshot, 2,
            "compaction does not advance the source snapshot"
        );
        let post_files = segment_files(&post);
        let superseded: Vec<_> = pre_files.difference(&post_files).cloned().collect();
        assert!(
            !superseded.is_empty(),
            "the compaction must actually supersede some segment files, else the test is vacuous"
        );

        // THE FIX: the superseded objects are RETAINED, so a reader pinned to the pre-compaction
        // layout can still fetch them.
        for rel in &superseded {
            assert!(
                object_exists(&op, prefix, rel).await,
                "same-snapshot re-layout must retain superseded object {rel} (a read-through reader \
                 pinned to it only reopens on a snapshot advance)"
            );
        }

        // A snapshot ADVANCE reclaims them — GC still works, it's just snapshot-gated.
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(vec![doc("c")], SourceCheckpoint::iceberg(3), "c"),
        )
        .unwrap();
        let adv = backup(&shard, "docs", "docs", &staging, &op, prefix, None)
            .await
            .unwrap();
        assert_eq!(adv.snapshot, 3);
        for rel in &superseded {
            assert!(
                !object_exists(&op, prefix, rel).await,
                "a snapshot advance past the pinned readers must reclaim superseded object {rel}"
            );
        }
    }
}
