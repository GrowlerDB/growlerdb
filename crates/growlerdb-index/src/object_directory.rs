//! A **read-only object-storage** [`tantivy::Directory`](Directory). It lets a tantivy
//! index whose files live in an opendal store be opened and queried *in place* — fetching only the
//! byte ranges a query touches — so cold (parked) time-window shards stay searchable without
//! restoring the whole window to local NVMe.
//!
//! tantivy's `Directory` reads are **synchronous**, so we drive opendal through its `blocking`
//! operator, which `block_on`s the current tokio runtime. That requires opening from within a
//! runtime context and reading from a *synchronous* one — exactly the shape the Search service
//! already uses (it runs query execution on `spawn_blocking`). Writes are unsupported: a cold
//! window is immutable.

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::ops::Range;
use std::path::Path;
use std::sync::{Arc, Mutex};

use opendal::blocking;
use opendal::options::ReadOptions;
use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    Directory, DirectoryLock, FileHandle, Lock, OwnedBytes, WatchCallback, WatchHandle, WritePtr,
};
use tantivy::HasLen;

use crate::bundle::BundleState;
use crate::range_cache::RangeCache;

/// Preloaded structural state so a cold open needs **zero object round-trips** (from the hotcache):
/// the full bytes of the tiny atomic files (`meta.json`, `.managed.json`), each file's length (so
/// `get_file_handle` can skip its `stat`), and the structural byte `ranges` each segment reader
/// touches on open. All **pinned** for the shard's lifetime — the ranges live here rather than in the
/// shared evictable [`RangeCache`], so other windows' traffic can't evict them and
/// silently re-introduce the cold round-trips the hotcache exists to avoid. Keyed by path relative to
/// the directory prefix; `ranges` maps a file → its `(start, bytes)` spans.
#[derive(Default)]
pub(crate) struct HotState {
    pub lens: HashMap<String, u64>,
    pub atomic: HashMap<String, Vec<u8>>,
    pub ranges: HashMap<String, Vec<(u64, OwnedBytes)>>,
}

/// Captures the structural reads a build-time warm-up performs so they can be packaged
/// into a hotcache. Present only on a directory opened with [`ObjectDirectory::recording`].
#[derive(Default)]
pub(crate) struct Recorder {
    pub lens: HashMap<String, u64>,
    pub atomic: HashMap<String, Vec<u8>>,
}

/// A read-only [`Directory`] over an opendal store, rooted at a key `prefix`.
#[derive(Clone)]
pub struct ObjectDirectory {
    op: blocking::Operator,
    /// Key prefix (with a trailing `/`) every file path is resolved under.
    prefix: String,
    /// Optional shared byte-range cache so repeat cold reads stay local.
    cache: Option<RangeCache>,
    /// Preloaded structural state served locally instead of hitting object storage.
    hot: Option<Arc<HotState>>,
    /// Build-time recorder; when set, structural reads are captured for the hotcache.
    rec: Option<Arc<Mutex<Recorder>>>,
    /// Split-bundle state: when set, file reads map to ranged GETs of one bundle object.
    bundle: Option<Arc<BundleState>>,
}

impl fmt::Debug for ObjectDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectDirectory")
            .field("prefix", &self.prefix)
            .finish()
    }
}

impl ObjectDirectory {
    /// Open the tantivy index rooted at `prefix` in the async `op`, read-only. Wraps `op` in a
    /// blocking operator that drives reads via the **current tokio runtime's** `block_on`, so this
    /// must be called from within a runtime context (it captures `Handle::current`); reads then run
    /// in synchronous context — which is exactly where tantivy search executes (the Search service
    /// runs it on `spawn_blocking`). Errors if there is no current runtime.
    pub fn open(op: opendal::Operator, prefix: impl Into<String>) -> Result<Self, opendal::Error> {
        Ok(Self {
            op: blocking::Operator::new(op)?,
            prefix: format!("{}/", prefix.into().trim_end_matches('/')),
            cache: None,
            hot: None,
            rec: None,
            bundle: None,
        })
    }

    /// Share a byte-range `cache` across this and other cold windows, so repeated reads
    /// (term dictionary, structural metadata, re-run queries) are served locally.
    pub fn with_cache(mut self, cache: RangeCache) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Serve open-time structural reads (atomic files + file lengths) from a preloaded hotcache
    /// instead of object storage, so opening a cold window needs zero round-trips.
    pub(crate) fn with_hot(mut self, hot: Arc<HotState>) -> Self {
        self.hot = Some(hot);
        self
    }

    /// Serve file reads from a single **bundle** object instead of one object per file:
    /// each read is mapped to a ranged GET of the bundle at the file's offset. Caching stays keyed by
    /// the per-file logical key, so this composes with [`Self::with_hot`].
    pub(crate) fn with_bundle(mut self, bundle: Arc<BundleState>) -> Self {
        self.bundle = Some(bundle);
        self
    }

    /// Record structural reads: every atomic-file body and file length this directory
    /// fetches is captured for packaging into a hotcache. Combine with [`Self::with_cache`] over a
    /// fresh cache to also capture the byte ranges.
    pub(crate) fn recording(mut self) -> Self {
        self.rec = Some(Arc::new(Mutex::new(Recorder::default())));
        self
    }

    /// Take the recorded structural reads (atomic bodies + lengths) after a warm-up; empty if this
    /// directory was not opened with [`Self::recording`].
    pub(crate) fn take_recorded(&self) -> Recorder {
        self.rec
            .as_ref()
            .map(|r| std::mem::take(&mut *r.lock().expect("recorder not poisoned")))
            .unwrap_or_default()
    }

    /// The object key for a tantivy file `path` (relative to the index dir).
    fn key(&self, path: &Path) -> String {
        format!("{}{}", self.prefix, rel(path))
    }

    /// If this directory is bundled and knows `rel`, its `(bundle key, offset, len)` — else `None`.
    fn bundle_span(&self, rel: &str) -> Option<(Arc<str>, u64, u64)> {
        let bundle = self.bundle.as_ref()?;
        let (offset, len) = bundle.files.get(rel).copied()?;
        Some((bundle.key.clone(), offset, len))
    }
}

/// A tantivy file path rendered relative to the directory prefix (forward-slashed), the key used in
/// the hotcache maps.
fn rel(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// Map an opendal error to a tantivy read error (a missing object ⇒ "file does not exist").
fn read_err(path: &Path, e: opendal::Error) -> OpenReadError {
    if e.kind() == opendal::ErrorKind::NotFound {
        OpenReadError::FileDoesNotExist(path.to_path_buf())
    } else {
        OpenReadError::wrap_io_error(io::Error::other(e), path.to_path_buf())
    }
}

fn read_only() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "ObjectDirectory is read-only (cold window)",
    )
}

/// A handle to one tantivy file, served by ranged GETs (cached when a cache is set). When the window
/// is **bundled**, the file's bytes live inside one shared bundle object at `phys_offset`,
/// but caching stays keyed by the per-file `cache_key` so hotcache-preloaded ranges still hit.
#[derive(Clone)]
struct ObjectFile {
    op: blocking::Operator,
    /// Logical key for the [`RangeCache`] — stable per tantivy file, bundled or not.
    cache_key: Arc<str>,
    /// Physical object to GET: the bundle (bundled) or the file's own object (not bundled).
    phys_key: Arc<str>,
    /// Byte offset of this file within `phys_key` (0 when not bundled).
    phys_offset: u64,
    len: usize,
    cache: Option<RangeCache>,
    /// Pinned hotcache spans for this file: `(start, bytes)`, served before the
    /// shared cache and never evicted.
    pinned: Vec<(u64, OwnedBytes)>,
}

impl fmt::Debug for ObjectFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectFile")
            .field("cache_key", &self.cache_key)
            .field("phys_key", &self.phys_key)
            .field("phys_offset", &self.phys_offset)
            .field("len", &self.len)
            .finish()
    }
}

impl HasLen for ObjectFile {
    fn len(&self) -> usize {
        self.len
    }
}

impl ObjectFile {
    /// Fetch `range` (relative to this file) from object storage — shifted into the physical object
    /// by `phys_offset` so a bundled file reads the right window of the shared bundle.
    fn fetch(&self, range: &Range<usize>) -> io::Result<OwnedBytes> {
        // Bound the read to this file's extent: in bundle mode the physical object is
        // the shared bundle, so a read past `len` (off-by-one / corruption) would silently return the
        // *adjacent* file's bytes as this file's data. Reject it instead of bleeding across files.
        if range.end > self.len || range.start > range.end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "read range {}..{} out of bounds for a {}-byte file",
                    range.start, range.end, self.len
                ),
            ));
        }
        let start = self.phys_offset + range.start as u64;
        let end = self.phys_offset + range.end as u64;
        let opts = ReadOptions {
            range: (start..end).into(),
            ..Default::default()
        };
        let buf = self
            .op
            .read_options(&self.phys_key, opts)
            .map_err(io::Error::other)?;
        Ok(OwnedBytes::new(buf.to_vec()))
    }
}

impl FileHandle for ObjectFile {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        // Pinned hotcache spans first: an exact (start, len) match is served from the
        // per-shard, never-evicted set — so a cold open's structural reads stay round-trip-free.
        for (start, bytes) in &self.pinned {
            if *start == range.start as u64 && bytes.len() == range.len() {
                return Ok(bytes.clone());
            }
        }
        let Some(cache) = &self.cache else {
            return self.fetch(&range);
        };
        if let Some(hit) = cache.get(&self.cache_key, &range) {
            return Ok(hit);
        }
        let bytes = self.fetch(&range)?;
        cache.put(&self.cache_key, &range, bytes.clone());
        Ok(bytes)
    }
}

impl Directory for ObjectDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let cache_key = self.key(path);
        let rel = rel(path);
        // Pinned hotcache spans for this file, if any.
        let pinned = self
            .hot
            .as_ref()
            .and_then(|h| h.ranges.get(&rel))
            .cloned()
            .unwrap_or_default();
        // Bundled: the file lives inside the one bundle object at a known offset+len — no stat.
        if let Some((phys_key, offset, len)) = self.bundle_span(&rel) {
            return Ok(Arc::new(ObjectFile {
                op: self.op.clone(),
                cache_key: Arc::from(cache_key),
                phys_key,
                phys_offset: offset,
                len: len as usize,
                cache: self.cache.clone(),
                pinned,
            }));
        }
        // Prefer a preloaded length (hotcache) → no `stat` round-trip on open.
        let len = match self.hot.as_ref().and_then(|h| h.lens.get(&rel).copied()) {
            Some(len) => len,
            None => {
                let len = self
                    .op
                    .stat(&cache_key)
                    .map_err(|e| read_err(path, e))?
                    .content_length();
                if let Some(rec) = &self.rec {
                    rec.lock()
                        .expect("recorder not poisoned")
                        .lens
                        .insert(rel, len);
                }
                len
            }
        };
        let key: Arc<str> = Arc::from(cache_key);
        Ok(Arc::new(ObjectFile {
            op: self.op.clone(),
            cache_key: key.clone(),
            phys_key: key,
            phys_offset: 0,
            len: len as usize,
            cache: self.cache.clone(),
            pinned,
        }))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let rel = rel(path);
        // Preloaded atomic bodies (meta.json / .managed.json) served locally on a hotcache open.
        if let Some(buf) = self.hot.as_ref().and_then(|h| h.atomic.get(&rel)) {
            return Ok(buf.clone());
        }
        // Bundled (and not in the hotcache): read the file's span out of the one bundle object.
        if let Some((phys_key, offset, len)) = self.bundle_span(&rel) {
            let opts = ReadOptions {
                range: (offset..offset + len).into(),
                ..Default::default()
            };
            return self
                .op
                .read_options(&phys_key, opts)
                .map(|b| b.to_vec())
                .map_err(|e| read_err(path, e));
        }
        match self.op.read(&self.key(path)) {
            Ok(buf) => {
                let buf = buf.to_vec();
                if let Some(rec) = &self.rec {
                    rec.lock()
                        .expect("recorder not poisoned")
                        .atomic
                        .insert(rel, buf.clone());
                }
                Ok(buf)
            }
            Err(e) => Err(read_err(path, e)),
        }
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        // A file known to the hotcache or bundle exists — skip the round-trip.
        let rel = rel(path);
        if let Some(hot) = &self.hot {
            if hot.lens.contains_key(&rel) || hot.atomic.contains_key(&rel) {
                return Ok(true);
            }
        }
        if self
            .bundle
            .as_ref()
            .is_some_and(|b| b.files.contains_key(&rel))
        {
            return Ok(true);
        }
        self.op
            .exists(&self.key(path))
            .map_err(|e| read_err(path, e))
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        Err(DeleteError::IoError {
            io_error: Arc::new(read_only()),
            filepath: path.to_path_buf(),
        })
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        Err(OpenWriteError::wrap_io_error(
            read_only(),
            path.to_path_buf(),
        ))
    }

    fn atomic_write(&self, _path: &Path, _data: &[u8]) -> io::Result<()> {
        Err(read_only())
    }

    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }

    /// No-op lock: a read-only directory can't (and needn't) write a lock file, but tantivy's
    /// `Index::open`/reload acquires `META_LOCK`. Hand back a lock that owns nothing.
    fn acquire_lock(&self, _lock: &Lock) -> Result<DirectoryLock, LockError> {
        Ok(DirectoryLock::from(Box::new(())))
    }

    /// No-op watch: cold windows are immutable, so there's nothing to notify on.
    fn watch(&self, _callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::collector::TopDocs;
    use tantivy::query::QueryParser;
    use tantivy::schema::{Schema, TEXT};
    use tantivy::{doc, Index};

    /// Build a tantivy index on local disk, copy its files into an fs-backed opendal store, then
    /// open and query it *through* `ObjectDirectory` — proving read-through serving returns the
    /// same hits without the files ever being on the searcher's local disk.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn opens_and_searches_a_tantivy_index_from_object_storage() {
        let mut sb = Schema::builder();
        let body = sb.add_text_field("body", TEXT);
        let schema = sb.build();

        let local = tempfile::tempdir().unwrap();
        let index = Index::create_in_dir(local.path(), schema).unwrap();
        let mut writer = index.writer(50_000_000).unwrap();
        writer.add_document(doc!(body => "alpha beta")).unwrap();
        writer.add_document(doc!(body => "beta gamma")).unwrap();
        writer.add_document(doc!(body => "delta")).unwrap();
        writer.commit().unwrap();

        // Copy the index files into an object store under prefix `cold/w1` (flat — tantivy writes
        // meta.json/.managed.json/segment files in one dir).
        let store_root = tempfile::tempdir().unwrap();
        let prefix_dir = store_root.path().join("cold/w1");
        std::fs::create_dir_all(&prefix_dir).unwrap();
        for entry in std::fs::read_dir(local.path()).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                std::fs::copy(entry.path(), prefix_dir.join(entry.file_name())).unwrap();
            }
        }

        let op = opendal::Operator::new(
            opendal::services::Fs::default().root(&store_root.path().to_string_lossy()),
        )
        .unwrap()
        .finish();
        // Open + search in synchronous context (spawn_blocking) — mirrors the Search service, and
        // the blocking operator's `block_on` requires a sync (not async) caller.
        let (beta, delta) = tokio::task::spawn_blocking(move || {
            let dir = ObjectDirectory::open(op, "cold/w1").unwrap();
            let index = Index::open(dir).unwrap();
            let searcher = index.reader().unwrap().searcher();
            let qp = QueryParser::for_index(&index, vec![body]);
            let beta = searcher
                .search(
                    &qp.parse_query("beta").unwrap(),
                    &TopDocs::with_limit(10).order_by_score(),
                )
                .unwrap()
                .len();
            let delta = searcher
                .search(
                    &qp.parse_query("delta").unwrap(),
                    &TopDocs::with_limit(10).order_by_score(),
                )
                .unwrap()
                .len();
            (beta, delta)
        })
        .await
        .unwrap();
        assert_eq!(beta, 2, "read-through search returns the matching docs");
        assert_eq!(delta, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cache_warms_on_repeat_search() {
        use crate::range_cache::RangeCache;

        let mut sb = Schema::builder();
        let body = sb.add_text_field("body", TEXT);
        let schema = sb.build();
        let local = tempfile::tempdir().unwrap();
        let index = Index::create_in_dir(local.path(), schema).unwrap();
        let mut writer = index.writer(50_000_000).unwrap();
        for w in ["alpha beta", "beta gamma", "delta"] {
            writer.add_document(doc!(body => w)).unwrap();
        }
        writer.commit().unwrap();

        let store_root = tempfile::tempdir().unwrap();
        let prefix_dir = store_root.path().join("cold/w1");
        std::fs::create_dir_all(&prefix_dir).unwrap();
        for entry in std::fs::read_dir(local.path()).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                std::fs::copy(entry.path(), prefix_dir.join(entry.file_name())).unwrap();
            }
        }
        let op = opendal::Operator::new(
            opendal::services::Fs::default().root(&store_root.path().to_string_lossy()),
        )
        .unwrap()
        .finish();
        let cache = RangeCache::new(8 * 1024 * 1024);

        let stats = tokio::task::spawn_blocking(move || {
            let dir = ObjectDirectory::open(op, "cold/w1")
                .unwrap()
                .with_cache(cache.clone());
            let index = Index::open(dir).unwrap();
            let qp = QueryParser::for_index(&index, vec![body]);
            let q = qp.parse_query("beta").unwrap();
            // Two identical searches: the second should hit the cache (no new fetches).
            let r1 = index
                .reader()
                .unwrap()
                .searcher()
                .search(&q, &TopDocs::with_limit(10).order_by_score())
                .unwrap();
            assert_eq!(r1.len(), 2);
            let warm = cache.stats();
            let r2 = index
                .reader()
                .unwrap()
                .searcher()
                .search(&q, &TopDocs::with_limit(10).order_by_score())
                .unwrap();
            assert_eq!(r2.len(), 2);
            (warm, cache.stats())
        })
        .await
        .unwrap();

        let (after_first, after_second) = stats;
        assert!(
            after_first.misses > 0,
            "first search fetched from object storage"
        );
        assert!(
            after_second.hits > after_first.hits,
            "second identical search served ranges from the cache"
        );
        assert!(after_second.cached_bytes > 0);
    }
}
