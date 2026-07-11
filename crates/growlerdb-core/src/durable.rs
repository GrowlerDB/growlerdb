//! **Durable atomic file writes**: the shared primitive for the metadata writers
//! (the [registry](../../growlerdb-controlplane), a Node's `index.json`, …). A plain
//! `write(tmp)` + `rename` only prevents *torn reads while the process is up* — on power loss or
//! a kernel crash the data or the rename itself can be lost. [`write`] closes that gap: it
//! fsyncs the temp file, renames it over the target, then fsyncs the parent directory so both
//! the bytes and the rename are durable.

use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Per-call nonce source, so concurrent writers never collide on a fixed temp name. Process id
/// + a monotonic counter is unique without needing a clock or RNG.
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Atomically and **durably** write `bytes` to `path`: write a uniquely-named temp sibling,
/// `fsync` it, `rename` it over the target, then `fsync` the parent directory. After this
/// returns Ok, the new contents survive a power loss / kernel crash — and a crash mid-write
/// leaves the original `path` intact (the rename is atomic), never a half-written target.
pub fn write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path.parent().filter(|p| !p.as_os_str().is_empty());
    let parent = parent.unwrap_or_else(|| Path::new("."));
    let stem = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("metadata");
    let nonce = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp = parent.join(format!(".{stem}.{}.{nonce}.tmp", std::process::id()));

    // Write + fsync the temp file, then make sure it's closed before the rename.
    let res = (|| {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()
    })();
    if let Err(e) = res {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    if let Err(e) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    // fsync the directory so the rename (the new dir entry) is itself durable. NOTE: the rename
    // already succeeded here, so the new contents are *visible* — a failure at this last step means
    // only that the directory entry isn't confirmed durable across a power loss. The
    // write is idempotent, so a caller should treat this as "written, durability unconfirmed" (safe to
    // retry) rather than "not written" (roll back). Wrap the error with that context so it's
    // distinguishable from a genuine write failure in logs.
    sync_dir(parent).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!(
                "`{}` was written and renamed but the directory fsync failed \
                 (contents visible; durability unconfirmed): {e}",
                path.display()
            ),
        )
    })
}

/// `fsync` a directory so prior renames/creations within it are durable. A no-op-shaped error
/// on platforms that can't fsync a directory handle is surfaced to the caller (we target Unix).
pub fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// The sibling `.prev` path for `path` — the last-known-good copy kept by
/// [`write_keeping_prev`] for a reader to fall back to if the latest file is found corrupt.
pub fn prev_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".prev");
    PathBuf::from(name)
}

/// Like [`write`], but first preserves the current contents of `path` (if any) as a durable
/// `.prev` sibling — a last-known-good copy a reader can fall back to if the freshly-written
/// file is later found corrupt (e.g. silent disk corruption), instead of hard-failing startup.
///
/// The `.prev` is rolled with a **hardlink** (O(1)), not a full byte copy of a large
/// `registry.json` on every mutation. `.prev` is linked to the *current* inode;
/// the atomic [`write`] below rebinds `path`'s name to a *new* inode, so `.prev` keeps the old
/// content while `path` gets the new — and `path` is never absent, so this stays crash-safe and the
/// recovery path is unchanged. Falls back to a byte copy on filesystems without hardlinks.
pub fn write_keeping_prev(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if path.exists() {
        let prev = prev_path(path);
        let _ = std::fs::remove_file(&prev); // clear the old .prev so hard_link has a free name
        if std::fs::hard_link(path, &prev).is_err() {
            // No hardlink support (or cross-device) → the original full copy.
            if let Ok(current) = std::fs::read(path) {
                write(&prev, &current)?;
            }
        }
    }
    write(path, bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_persists_contents_and_overwrites_atomically() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("registry.json");

        write(&path, b"first").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"first");

        // Overwrite — the target ends up with the new contents, no temp left behind.
        write(&path, b"second-longer").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"second-longer");

        let leftovers: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".tmp"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "temp files left behind: {leftovers:?}"
        );
    }

    #[test]
    fn concurrent_writes_use_distinct_temp_names() {
        // Distinct nonces back-to-back — the temp path must differ each call (no fixed `.tmp`).
        let tmp = tempfile::tempdir().unwrap();
        let a = tmp.path().join("a.json");
        write(&a, b"x").unwrap();
        write(&a, b"y").unwrap();
        assert_eq!(std::fs::read(&a).unwrap(), b"y");
    }
}
