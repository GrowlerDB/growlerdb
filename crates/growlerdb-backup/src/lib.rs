//! **Object-storage backup & restore** for a shard's index. A backup ships a shard's
//! consistent committed state — sealed Tantivy segments + the `location.arr` array + the
//! `aux.redb` aux store + the index definition — to object storage; a restore pulls it back onto
//! a replacement node, which then
//! **replays the tail from the backed-up checkpoint** via normal ingestion. With no backup, the
//! shard is rebuilt from Iceberg (the engine's `rebuild`) — no GrowlerDB state is irreplaceable.
//!
//! Transport is [`opendal`]: **S3/MinIO** in production (`s3_store`) and a local **filesystem**
//! service (`fs_store`) for backup-to-a-mounted-volume and for tests — so the logic is verifiable
//! without a live object store. Layout under a backup `prefix`:
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

/// The manifest **format version** this binary writes and consumes. Format **1** is the
/// layered-locator shard format — the file list carries `location.arr` beside the
/// segments and `aux.redb`. The version field + the refuse-newer check in [`read_manifest`]
/// are release hygiene: a future incompatible layout bumps this, and older binaries fail
/// loudly instead of mis-restoring.
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

/// Wrap `op` with a **jittered retry layer**. Object stores routinely return
/// transient errors — S3 `503 SlowDown`, 5xx, connection resets — under exactly the load GrowlerDB
/// generates (shipping a many-file shard, scanning a big snapshot). Without this a single blip aborts
/// the whole backup / restore / replica-refresh mid-flight, and the non-transactional file-then-
/// manifest write can leave a partial prefix. opendal only retries errors it marks *temporary*, so
/// terminal failures (`NotFound`, auth) still surface immediately. Jitter avoids a synchronized retry
/// herd across a fleet.
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
        .region(&cfg.region)
        .access_key_id(&cfg.access_key_id)
        .secret_access_key(&cfg.secret_access_key);
    if let Some(ep) = &cfg.endpoint {
        b = b.endpoint(ep);
    }
    Ok(with_retry(Operator::new(b)?.finish()))
}

/// An [`Operator`] over a local directory — a filesystem backup target (mounted volume / NFS),
/// and the backend the tests use. Retries transient failures (NFS can blip too).
pub fn fs_store(root: impl AsRef<Path>) -> Result<Operator> {
    let root = root.as_ref();
    std::fs::create_dir_all(root)?;
    let b = opendal::services::Fs::default().root(&root.to_string_lossy());
    Ok(with_retry(Operator::new(b)?.finish()))
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
    // Written LAST — its presence is the "backup is complete and restorable" commit point.
    store
        .write(
            &format!("{prefix}/manifest.json"),
            serde_json::to_vec(&manifest)?,
        )
        .await?;

    // Backup GC: prune superseded splits from object storage. Every compaction on the
    // primary fuses segments into new, differently-named files; the old segment objects under
    // `{prefix}/data/` are no longer referenced by any manifest, and re-backing-up to the same
    // prefix only *adds* the new files — so without this the store accumulates orphaned splits
    // forever. Mirrors refresh()'s local prune on the remote side. Run AFTER the manifest commit:
    // a crash here leaves a valid manifest plus a few orphans, which the next backup's GC reclaims.
    prune_superseded(store, prefix, &manifest).await?;

    let _ = std::fs::remove_dir_all(staging);
    Ok(manifest)
}

/// Delete objects under `{prefix}/data/` that the just-committed `manifest` no longer references —
/// the remote counterpart of [`refresh`]'s local prune. Idempotent and safe to re-run: it only
/// removes keys absent from the manifest's file set, so the restorable state (manifest + its files)
/// is never touched. Returns the number of objects pruned.
///
/// **Precondition — single writer per prefix.** GrowlerDB backs a shard up from its
/// **one** primary, so there is exactly one writer per backup prefix. Two concurrent `backup()`s
/// against the same prefix (e.g. a split-brain "both primary") could have one's prune delete a file
/// the other just committed. That precondition holds by the shard-ownership model; the safety net for
/// a replica that read an older manifest and races this prune is [`refresh`]'s re-read-and-retry on a
/// mid-flight `NotFound`.
/// List every **object** key under `prefix` (recursive), filtering out the trailing-slash directory
/// markers the fs backend emits. The shared scan behind prune / bundle-delete /
/// promote — each of those only differs in what it does per key, not in how it enumerates them.
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

async fn prune_superseded(store: &Operator, prefix: &str, manifest: &Manifest) -> Result<usize> {
    let data_prefix = format!("{prefix}/data/");
    let wanted: std::collections::HashSet<&str> =
        manifest.files.iter().map(|f| f.path.as_str()).collect();
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
/// `aux.redb` + `location.arr` (the cold footprint `open_cold_shard` still reads). The LAST step of a
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
    // Split bundle: concatenate the parked index files into ONE object so cold queries
    // issue ranged GETs against a single object instead of one per file. On success the now-redundant
    // individual index objects are removed — the bundle is the sole serving copy, so no storage
    // doubling — and open falls to the bundle for both structural and posting reads. On failure we
    // keep the individual files and fall back to plain per-file read-through. Stored OUTSIDE `data/`
    // so backup GC won't touch it. Built AFTER the hotcache (which reads the individual files).
    // Bundle from the LOCAL window files: they're still on disk here (eviction is the last
    // step), so stream them straight into the split object instead of re-downloading the whole window
    // from the store. The backup manifest lists exactly what was parked; the `index/` entries (stripped
    // of that prefix) are the bare rels the bundle records, read from `window_dir/index`.
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
                // Commit the `bundled` manifest BEFORE deleting the per-file objects. The old
                // order (delete, then best-effort rewrite) left a crash window where the durable
                // manifest still listed the deleted `index/*` objects as restorable — a later
                // `restore` 404'd mid-download instead of getting the clean `Bundled` refusal.
                // With manifest-first, every crash point is consistent: rewrite fails ⇒ objects
                // are kept and the old manifest still restores; rewrite lands ⇒ the objects are
                // unreferenced and their deletion is pure (best-effort) reclamation.
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
    let marker = ColdMarker {
        object_prefix,
        event_min: zone.map(|(lo, _)| lo),
        event_max: zone.map(|(_, hi)| hi),
        snapshot: manifest.snapshot,
        hotcache_key,
        bundle_key,
        bundle_manifest_key,
    };
    std::fs::write(
        window_dir.join(growlerdb_index::COLD_MARKER),
        serde_json::to_vec_pretty(&marker)?,
    )?;
    Ok(marker)
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
        let _ = store.remove_all(base).await;
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
    // * The pass **tears**: the mutable objects (`index/meta.json`, `aux.redb`, `location.arr`)
    //   are fetched live while segment files come from the manifest's list, so a backup landing
    //   mid-pass can pair a NEWER meta with the OLDER segment set — a meta referencing segments
    //   never downloaded (and the prune step even removes files the new meta needs). The
    //   manifest is the backup's commit point (written last), so re-reading it after the pass
    //   and comparing snapshots detects any backup that completed during the pass; a retry is
    //   cheap (the immutable segments already downloaded are reused). A sub-object-read race
    //   narrower than the manifest commit remains theoretically possible but is bounded by one
    //   GET, not the whole multi-second pass.
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
        // The index meta + aux store change every commit — and `location.arr` is
        // patched **in place** (same length, new bytes), so the size check can't
        // detect a change; segment files are immutable.
        let mutable = matches!(
            entry.path.as_str(),
            "aux.redb" | "index/meta.json" | "index/.managed.json" | "location.arr"
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

/// One **live read-replica** refresh cycle: [`refresh`] the replica's `shard_id` shard in
/// `store`, and re-open it **only when the primary has actually moved on** — returning the fresh
/// shard for the caller to hot-swap (e.g. `ShardHandle::swap`). The primary's `meta.json`/`aux.redb`
/// re-download every poll (they're mutable), so the authoritative "something changed" signal is the
/// backup's **snapshot** advancing past the `served_snapshot` the replica is currently serving. (The
/// raw `RefreshStats` counts can't tell idle from changed: the mutable meta/locator always count as
/// downloaded, and opening the shard writes a local writer-lock that the next refresh prunes — so
/// only the snapshot is reliable. A primary compaction at the same snapshot leaves results
/// unchanged, so skipping its re-open is correct; the next real commit picks up the merged layout.)
/// On a snapshot advance, the definition is re-materialized at `def_path` (if the manifest carries
/// one and `def_path` is set) so the replica tracks the primary's schema, and the shard is re-opened.
/// `Ok((None, stats))` means the replica was already up to date, so a steady-state poll never
/// re-opens. The swap is the caller's (serving) concern, keeping this pure of the server loop and
/// unit-testable against an `fs` backup.
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
            iceberg_file: "f".into(),
            row_position: 0,
        }
    }

    /// The torn-refresh hazard and its guard. A refresh pass fetches the mutable objects
    /// (`index/meta.json`, `aux.redb`, `location.arr`) live while segment files come from the
    /// manifest's list — so a pass running against a **stale** manifest while the store already
    /// holds a newer backup assembles a shard whose meta references segments it never
    /// downloaded (and prunes ones it shouldn't). `refresh_once` (the raw pass) reproduces
    /// exactly that; the public [`refresh`] re-reads the manifest after the pass and retries,
    /// converging on a consistent, openable shard.
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
}
