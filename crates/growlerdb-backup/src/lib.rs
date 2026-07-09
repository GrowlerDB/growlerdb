//! **Object-storage backup & restore** for a shard's index (task-32). A backup ships a shard's
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
    /// The prefix is a **bundled** cold window (task-150 / F7): its data lives in the split bundle,
    /// not per-file objects, so it can't be `restore`d — un-bundle it (`promote_cold`) instead.
    #[error("prefix `{0}` is a bundled cold window; un-bundle (promote) it rather than restore")]
    Bundled(String),
    /// The manifest declares a [format](Manifest::format) newer than this binary supports (D30
    /// foundations): the backup was written by a newer GrowlerDB whose layout this version can't
    /// interpret, so refuse loudly rather than mis-restore.
    #[error(
        "backup manifest format {found} is newer than the supported format {supported}: this \
         backup was written by a newer GrowlerDB — restore it with a matching GrowlerDB version"
    )]
    UnsupportedFormat { found: u32, supported: u32 },
}

type Result<T> = std::result::Result<T, BackupError>;

/// The manifest **format version** this binary writes and consumes. Format **1** IS the
/// D30 layered-locator shard format — the file list carries `location.arr` beside the
/// segments and `aux.redb`. (GrowlerDB is unreleased, so 1 was reset to mean the layered
/// format; there is no earlier on-disk format in the wild.) The version field + the
/// refuse-newer check in [`read_manifest`] are release hygiene: a future incompatible
/// layout bumps this, and older binaries fail loudly instead of mis-restoring.
pub const MANIFEST_FORMAT: u32 = 1;

/// Manifests written without a `format` field deserialize as format 1.
fn default_manifest_format() -> u32 {
    1
}

/// What a backup recorded — enough to restore the shard and resume ingestion exactly-once.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Manifest **format version** (D30 foundations): bumped on incompatible layout changes.
    /// Every consumer goes through [`read_manifest`], which refuses formats newer than
    /// [`MANIFEST_FORMAT`] with [`BackupError::UnsupportedFormat`]. Defaulted to 1 so
    /// pre-versioning manifests keep restoring.
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
    /// Set once a cold window has been **bundled** (task-150 / F7): the individual `index/*` data
    /// objects were removed and their bytes now live in the split bundle, so this manifest's file
    /// list no longer resolves against `<prefix>/data/`. A plain [`restore`] refuses such a prefix
    /// (it must be un-bundled — [`promote_cold`] does). Defaulted for pre-flag manifests.
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

/// Wrap `op` with a **jittered retry layer** (task-149 / F9). Object stores routinely return
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
/// host style stays off unless explicitly enabled). Retries transient failures (task-149 / F9).
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
/// and the backend the tests use. Retries transient failures (task-149 / F9; NFS can blip too).
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

    // Backup GC (task-33): prune superseded splits from object storage. Every compaction on the
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
/// **Precondition — single writer per prefix (task-149 / I7).** GrowlerDB backs a shard up from its
/// **one** primary, so there is exactly one writer per backup prefix. Two concurrent `backup()`s
/// against the same prefix (e.g. a split-brain "both primary") could have one's prune delete a file
/// the other just committed. That precondition holds by the shard-ownership model; the safety net for
/// a replica that read an older manifest and races this prune is [`refresh`]'s re-read-and-retry on a
/// mid-flight `NotFound`.
/// List every **object** key under `prefix` (recursive), filtering out the trailing-slash directory
/// markers the fs backend emits (task-153 / D3). The shared scan behind prune / bundle-delete /
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
/// reclaiming a superseded prefix where a straggler is harmless (task-153 / D3). Callers needing a
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

/// Park a **cold** shard for tiered storage (task-80): back it up to `store` under `prefix`, then —
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

/// Revive a parked shard (task-80): restore the backup at `prefix` back into `shard_dir` — the
/// inverse of [`park`]. A thin wrapper over [`restore`] named for the cold-tiering lifecycle; the
/// caller then opens the shard and ingestion replays the tail from the manifest checkpoint.
pub async fn revive(store: &Operator, prefix: &str, shard_dir: &Path) -> Result<Manifest> {
    restore(store, prefix, shard_dir).await
}

/// **Cold-park** a window shard for *read-through* serving (task-80): back its bulk up to `store`
/// under `prefix`, then evict only the local Tantivy `index/` dir while **keeping `aux.redb`**, and
/// drop a [`ColdMarker`] in `window_dir`. Unlike [`park`] (full evict → unqueryable until restored),
/// the window stays **searchable in place** — `open_cold_shard` serves the index read-through from
/// `<prefix>/data/index` with the local aux. Returns the marker (the caller writes it via the index
/// store / uses it to register the window). The shard is consumed so its handles close before the
/// `index/` dir is removed.
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
    // The event-time zone-map travels into the marker so the gateway can prune a cold window
    // without opening it.
    let zone = shard.event_bounds()?;
    let mut manifest = backup(
        &shard,
        index,
        &format!("w{window}"),
        staging,
        store,
        prefix,
        definition_json,
    )
    .await?;
    // Backup committed → close handles (redb + tantivy) before touching the directory. The local
    // `index/` is NOT evicted yet: eviction is the LAST step (after the marker is durable) so a crash
    // mid-park leaves a fully-serving hot shard, never a markerless empty window (task-150 / F4).
    drop(shard);
    let base = prefix.trim_end_matches('/');
    let object_prefix = format!("{base}/data/index");
    // Precomputed hotcache (task-83): warm the just-parked index once and store the structural reads
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
    // Split bundle (task-83): concatenate the parked index files into ONE object so cold queries
    // issue ranged GETs against a single object instead of one per file. On success the now-redundant
    // individual index objects are removed — the bundle is the sole serving copy, so no storage
    // doubling — and open falls to the bundle for both structural and posting reads. On failure we
    // keep the individual files and fall back to plain per-file read-through. Stored OUTSIDE `data/`
    // so backup GC won't touch it. Built AFTER the hotcache (which reads the individual files).
    // Bundle from the LOCAL window files (task-155): they're still on disk here (eviction is the last
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
                delete_prefix_best_effort(store, &format!("{object_prefix}/")).await;
                // Keep the committed manifest consistent with the store (task-150 / F7): the
                // individual `index/*` objects are gone, so mark it `bundled` and drop those entries
                // (keep `aux.redb`) — a plain `restore` of this prefix now refuses cleanly instead of
                // 404-ing mid-download. Best-effort: the cold window serves from the bundle regardless.
                manifest.bundled = true;
                manifest.files.retain(|f| !f.path.starts_with("index/"));
                if let Ok(bytes) = serde_json::to_vec(&manifest) {
                    let _ = store.write(&format!("{base}/manifest.json"), bytes).await;
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
    // Evict the local bulk LAST, now that the marker is durably written (task-150 / F4) — before this
    // point a crash leaves a hot shard; after it, discovery serves the window cold read-through.
    // `aux.redb` stays as the cold footprint.
    let index_subdir = window_dir.join("index");
    if index_subdir.exists() {
        std::fs::remove_dir_all(&index_subdir)?;
    }
    Ok(marker)
}

/// Promote a cold (read-through) window back to a **local hot shard** (task-83 pre-warm): materialize
/// its Tantivy index files locally under `window_dir/index` — from the split bundle when present, else
/// the individual objects (pre-bundle windows) — then drop the `cold.json` marker. The window's
/// `aux.redb` is already local, so afterward `open_shard` opens a normal on-NVMe hot shard with no
/// cold latency; the caller swaps it into the live handle. On success the window's now-unused
/// object-storage copies (bundle / hotcache / backup) are reclaimed (task-150 / B9), which also
/// mops up any `data/index/*` orphaned by a crashed bundle-delete (I5).
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
            // Pre-bundle cold window: pull the individual index objects down.
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
    // Reclaim the window's object-storage copies now that it's served locally (task-150 / B9, I5):
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
/// manifest [format](Manifest::format) newer than [`MANIFEST_FORMAT`] is refused (D30 foundations):
/// a newer layout can't be interpreted here, and failing loudly beats mis-restoring.
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
    // a per-file restore can't rebuild it — refuse cleanly rather than 404 mid-download (task-150 /
    // F7). Such a window is un-bundled by `promote_cold`, not restored.
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

/// Refresh a **replica** shard at `dest` from the primary's backup at `prefix` — D14 segment
/// shipping: the replica *pulls sealed segments* rather than re-indexing the source. Incremental:
/// immutable segment files already present (same path + size) are skipped; the mutable
/// `meta.json` / `.managed.json` / `aux.redb` are always re-fetched; and local index files no
/// longer in the manifest (compacted away on the primary) are pruned. Because segments are copied
/// **byte-for-byte**, a replica scores identically to the primary. The caller (re)opens the shard
/// afterward; the first refresh of an empty `dest` downloads everything.
pub async fn refresh(store: &Operator, prefix: &str, dest: &Path) -> Result<RefreshStats> {
    let manifest = read_manifest(store, prefix).await?;
    match refresh_once(store, prefix, dest, manifest).await {
        // A listed segment 404'd mid-download: a concurrent backup's GC (prune_superseded) pruned a
        // file this now-stale manifest still names (task-149 / I7). Re-read the manifest and retry
        // once against the current file set; a second NotFound is a real error.
        Err(BackupError::Store(e)) if e.kind() == opendal::ErrorKind::NotFound => {
            let manifest = read_manifest(store, prefix).await?;
            refresh_once(store, prefix, dest, manifest).await
        }
        result => result,
    }
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

/// One **live read-replica** refresh cycle (task-31): [`refresh`] the replica's `shard_id` shard in
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
