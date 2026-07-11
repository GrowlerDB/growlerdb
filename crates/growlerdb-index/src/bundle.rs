//! **Split bundling** for cold windows. The cold tier parks each Tantivy segment file
//! as its own object, so a cold query that touches several files issues one ranged GET *per file*
//! (term dict, postings, fast fields, store — easily 5–15 objects per segment). Bundling
//! concatenates a window's files into a single "split" object and records each file's byte span, so
//! the read path issues ranged GETs against **one** object instead — cutting per-query request
//! fan-out (and the per-object latency/overhead), the Quickwit split model.
//!
//! The bundle composes with the [`hotcache`](crate::hotcache): the hotcache still serves structural
//! reads on open with zero round-trips (its ranges are keyed by the *logical* per-file key, stable
//! across bundling), and only a query's actual cold postings are fetched — now from the one bundle
//! object at the file's offset.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::store::{Result, StoreError};

/// The layout of a bundled split: where each index file lives within the single bundle object.
/// Serialized (postcard) alongside the bundle so a cold open is self-describing.
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq, Eq)]
pub struct BundleLayout {
    /// `(relative file, offset, len)` — each index file's byte span within the bundle object.
    pub files: Vec<(String, u64, u64)>,
}

/// Serving-side bundle state on an [`ObjectDirectory`](crate::ObjectDirectory): the bundle object key
/// plus each file's `(offset, len)`, so a file read maps to a ranged GET of the bundle at its offset.
pub(crate) struct BundleState {
    pub key: Arc<str>,
    pub files: HashMap<String, (u64, u64)>,
}

/// Build a single bundle from the individual index files under `object_prefix` in `op`: concatenate
/// them into one object at `bundle_key` and write the [`BundleLayout`] (postcard) to `manifest_key`.
/// Async object I/O only (no tantivy), so it runs directly on the caller's runtime. Files are read in
/// listing order; Tantivy addresses them by name, so order is irrelevant to correctness.
pub async fn build(
    op: &opendal::Operator,
    object_prefix: &str,
    bundle_key: &str,
    manifest_key: &str,
) -> Result<BundleLayout> {
    let base = format!("{}/", object_prefix.trim_end_matches('/'));
    let mut files: Vec<(String, u64, u64)> = Vec::new();
    let listed = op
        .list_with(&base)
        .recursive(true)
        .await
        .map_err(|e| StoreError::Cold(e.to_string()))?;
    // Stream each file through the object writer (multipart on S3) rather than concatenating the
    // whole window into one Vec<u8> — a cold window is potentially many GB, so buffering it all would
    // OOM the node at park time. Peak memory is one segment file, not the window.
    let mut writer = op
        .writer(bundle_key)
        .await
        .map_err(|e| StoreError::Cold(e.to_string()))?;
    let mut offset: u64 = 0;
    for entry in listed {
        let key = entry.path();
        if key.ends_with('/') {
            continue; // directory marker (fs backend)
        }
        let rel = key.strip_prefix(base.as_str()).unwrap_or(key).to_string();
        let bytes = op
            .read(key)
            .await
            .map_err(|e| StoreError::Cold(e.to_string()))?
            .to_vec();
        let len = bytes.len() as u64;
        writer
            .write(bytes)
            .await
            .map_err(|e| StoreError::Cold(e.to_string()))?;
        files.push((rel, offset, len));
        offset += len;
    }
    writer
        .close()
        .await
        .map_err(|e| StoreError::Cold(e.to_string()))?;
    let layout = BundleLayout { files };
    write_bundle_manifest(op, manifest_key, &layout).await?;
    Ok(layout)
}

/// Frame (postcard) + write a [`BundleLayout`] to `manifest_key`, so a cold open is self-describing.
async fn write_bundle_manifest(
    op: &opendal::Operator,
    manifest_key: &str,
    layout: &BundleLayout,
) -> Result<()> {
    let manifest =
        crate::sidecar::frame(crate::sidecar::BUNDLE_MAGIC, postcard::to_stdvec(layout)?);
    op.write(manifest_key, manifest)
        .await
        .map_err(|e| StoreError::Cold(e.to_string()))?;
    Ok(())
}

/// Like [`build`], but streams the window's index files straight from a **local directory** instead of
/// re-downloading them from object storage. At cold-park the files are still on local disk
/// (eviction is the last step), so re-fetching each one from the store just to concatenate it is pure
/// I/O waste. `files` are paths relative to `local_dir` — pass the backup manifest's `index/` files
/// (stripped of that prefix) so the bundle records the same bare rels as [`build`] and contains
/// exactly what was parked. Peak memory stays one file: each is streamed through the multipart writer
/// and dropped, matching [`build`]'s bound. The resulting [`BundleLayout`] is byte-identical in shape
/// to a [`build`] over the same files (offsets follow `files` order; Tantivy addresses by name, so
/// order is irrelevant to correctness).
pub async fn build_from_dir(
    op: &opendal::Operator,
    local_dir: &std::path::Path,
    files: &[String],
    bundle_key: &str,
    manifest_key: &str,
) -> Result<BundleLayout> {
    let mut writer = op
        .writer(bundle_key)
        .await
        .map_err(|e| StoreError::Cold(e.to_string()))?;
    let mut recorded: Vec<(String, u64, u64)> = Vec::with_capacity(files.len());
    let mut offset: u64 = 0;
    for rel in files {
        let bytes = std::fs::read(local_dir.join(rel))?;
        let len = bytes.len() as u64;
        writer
            .write(bytes)
            .await
            .map_err(|e| StoreError::Cold(e.to_string()))?;
        recorded.push((rel.clone(), offset, len));
        offset += len;
    }
    writer
        .close()
        .await
        .map_err(|e| StoreError::Cold(e.to_string()))?;
    let layout = BundleLayout { files: recorded };
    write_bundle_manifest(op, manifest_key, &layout).await?;
    Ok(layout)
}

/// Un-bundle a split back into individual files under `dest_index_dir` (for pre-warm): read
/// the layout from `manifest_key`, then **ranged-read each file's span** from the bundle straight to
/// disk. The inverse of [`build`] — used to promote a bundled cold window back to a local hot shard.
/// Ranged per-file reads (not one whole-bundle fetch) keep peak memory to one segment, so promoting a
/// multi-GB window can't OOM the node. Async object I/O only.
pub async fn unbundle(
    op: &opendal::Operator,
    bundle_key: &str,
    manifest_key: &str,
    dest_index_dir: &std::path::Path,
) -> Result<()> {
    let manifest = op
        .read(manifest_key)
        .await
        .map_err(|e| StoreError::Cold(e.to_string()))?
        .to_vec();
    let layout: BundleLayout = postcard::from_bytes(crate::sidecar::unframe(
        crate::sidecar::BUNDLE_MAGIC,
        &manifest,
    )?)?;
    std::fs::create_dir_all(dest_index_dir)?;
    for (rel, offset, len) in &layout.files {
        let buf = op
            .read_with(bundle_key)
            .range(*offset..*offset + *len)
            .await
            .map_err(|e| StoreError::Cold(e.to_string()))?;
        let dst = dest_index_dir.join(rel);
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }
        growlerdb_core::durable::write(&dst, &buf.to_vec())?;
    }
    Ok(())
}

impl BundleState {
    /// Reconstitute serving state from a bundle object `key` and its serialized [`BundleLayout`].
    pub(crate) fn from_bytes(key: &str, manifest_bytes: &[u8]) -> Result<Self> {
        let layout: BundleLayout = postcard::from_bytes(crate::sidecar::unframe(
            crate::sidecar::BUNDLE_MAGIC,
            manifest_bytes,
        )?)?;
        Ok(BundleState {
            key: Arc::from(key),
            files: layout
                .files
                .into_iter()
                .map(|(f, o, l)| (f, (o, l)))
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_directory::ObjectDirectory;
    use crate::range_cache::RangeCache;
    use tantivy::collector::TopDocs;
    use tantivy::query::QueryParser;
    use tantivy::schema::{Schema, TEXT};
    use tantivy::{doc, Index};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bundles_index_into_one_object_and_serves_reads_from_it() {
        // Stage a tantivy index as individual objects under cold/w1.
        let mut sb = Schema::builder();
        let body = sb.add_text_field("body", TEXT);
        let local = tempfile::tempdir().unwrap();
        let index = Index::create_in_dir(local.path(), sb.build()).unwrap();
        let mut writer = index.writer(50_000_000).unwrap();
        for w in ["alpha beta", "beta gamma", "delta", "alpha delta"] {
            writer.add_document(doc!(body => w)).unwrap();
        }
        writer.commit().unwrap();
        let store_root = tempfile::tempdir().unwrap();
        let prefix_dir = store_root.path().join("cold/w1");
        std::fs::create_dir_all(&prefix_dir).unwrap();
        let mut file_count = 0;
        for entry in std::fs::read_dir(local.path()).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                std::fs::copy(entry.path(), prefix_dir.join(entry.file_name())).unwrap();
                file_count += 1;
            }
        }
        assert!(file_count > 1, "index has several files to bundle");
        let op = opendal::Operator::new(
            opendal::services::Fs::default().root(&store_root.path().to_string_lossy()),
        )
        .unwrap()
        .finish();

        // Bundle them into one object + a layout manifest.
        let layout = build(&op, "cold/w1", "cold/split.bundle", "cold/split.manifest")
            .await
            .unwrap();
        assert_eq!(
            layout.files.len(),
            file_count,
            "every file is in the bundle"
        );

        // Remove the individual objects — the bundle must be self-sufficient for serving.
        std::fs::remove_dir_all(&prefix_dir).unwrap();

        let manifest = op.read("cold/split.manifest").await.unwrap().to_vec();
        let (first_rel, first_len) = (layout.files[0].0.clone(), layout.files[0].2 as usize);
        let hits = tokio::task::spawn_blocking(move || {
            use tantivy::directory::Directory;
            let state = BundleState::from_bytes("cold/split.bundle", &manifest).unwrap();
            let dir = ObjectDirectory::open(op, "cold/w1")
                .unwrap()
                .with_cache(RangeCache::new(16 * 1024 * 1024))
                .with_bundle(Arc::new(state));
            // A read past a bundled file's end must error, not bleed the adjacent
            // file's bytes out of the shared bundle object.
            let h = dir
                .get_file_handle(std::path::Path::new(&first_rel))
                .unwrap();
            assert!(h.read_bytes(0..first_len).is_ok(), "in-bounds read ok");
            assert!(
                h.read_bytes(0..first_len + 1).is_err(),
                "over-range read is rejected, not bled from the next file"
            );
            let index = Index::open(dir).unwrap();
            let body = index.schema().get_field("body").unwrap();
            let qp = QueryParser::for_index(&index, vec![body]);
            index
                .reader()
                .unwrap()
                .searcher()
                .search(
                    &qp.parse_query("alpha").unwrap(),
                    &TopDocs::with_limit(10).order_by_score(),
                )
                .unwrap()
                .len()
        })
        .await
        .unwrap();
        assert_eq!(
            hits, 2,
            "cold window served entirely from the single bundle object"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unbundle_reconstructs_a_searchable_local_index() {
        // Bundle an index, then un-bundle it back to local files and open it directly (the pre-warm
        // promote path): the reconstructed index must search identically.
        let mut sb = Schema::builder();
        let body = sb.add_text_field("body", TEXT);
        let local = tempfile::tempdir().unwrap();
        let index = Index::create_in_dir(local.path(), sb.build()).unwrap();
        let mut writer = index.writer(50_000_000).unwrap();
        for w in ["alpha beta", "beta gamma", "delta", "alpha delta"] {
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
        build(&op, "cold/w1", "cold/split.bundle", "cold/split.manifest")
            .await
            .unwrap();

        // Un-bundle into a fresh local dir and open it straight off disk.
        let dest = tempfile::tempdir().unwrap();
        unbundle(&op, "cold/split.bundle", "cold/split.manifest", dest.path())
            .await
            .unwrap();
        let index = Index::open_in_dir(dest.path()).unwrap();
        let body = index.schema().get_field("body").unwrap();
        let qp = QueryParser::for_index(&index, vec![body]);
        let hits = index
            .reader()
            .unwrap()
            .searcher()
            .search(
                &qp.parse_query("alpha").unwrap(),
                &TopDocs::with_limit(10).order_by_score(),
            )
            .unwrap()
            .len();
        assert_eq!(hits, 2, "un-bundled local index searches identically");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn build_from_dir_bundles_local_files_without_re_download() {
        // The cold-park path: the window's index files are still on local disk, so the
        // bundle is built straight from them (no round-trip through the store). Verify the bundle it
        // writes un-bundles back into a searchable index — i.e. equivalent to a store-side build.
        let mut sb = Schema::builder();
        let body = sb.add_text_field("body", TEXT);
        let local = tempfile::tempdir().unwrap();
        let index = Index::create_in_dir(local.path(), sb.build()).unwrap();
        let mut writer = index.writer(50_000_000).unwrap();
        for w in ["alpha beta", "beta gamma", "delta", "alpha delta"] {
            writer.add_document(doc!(body => w)).unwrap();
        }
        writer.commit().unwrap();
        drop(writer); // release the writer lock, as cold_park drops the shard before bundling

        // The file list as cold_park passes it: the parked index files, relative to the local dir.
        let files: Vec<String> = std::fs::read_dir(local.path())
            .unwrap()
            .filter_map(|e| {
                let e = e.unwrap();
                e.file_type()
                    .unwrap()
                    .is_file()
                    .then(|| e.file_name().to_string_lossy().into_owned())
            })
            .collect();
        assert!(files.len() > 1, "index has several files to bundle");

        let store_root = tempfile::tempdir().unwrap();
        let op = opendal::Operator::new(
            opendal::services::Fs::default().root(&store_root.path().to_string_lossy()),
        )
        .unwrap()
        .finish();

        let layout = build_from_dir(
            &op,
            local.path(),
            &files,
            "cold/split.bundle",
            "cold/split.manifest",
        )
        .await
        .unwrap();
        assert_eq!(
            layout.files.len(),
            files.len(),
            "every local file is bundled"
        );

        // Un-bundle into a fresh dir and open it straight off disk — searches identically.
        let dest = tempfile::tempdir().unwrap();
        unbundle(&op, "cold/split.bundle", "cold/split.manifest", dest.path())
            .await
            .unwrap();
        let index = Index::open_in_dir(dest.path()).unwrap();
        let body = index.schema().get_field("body").unwrap();
        let qp = QueryParser::for_index(&index, vec![body]);
        let hits = index
            .reader()
            .unwrap()
            .searcher()
            .search(
                &qp.parse_query("alpha").unwrap(),
                &TopDocs::with_limit(10).order_by_score(),
            )
            .unwrap()
            .len();
        assert_eq!(hits, 2, "bundle built from local files serves correctly");
    }
}
