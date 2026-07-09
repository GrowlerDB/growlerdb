//! The **location store** — the mutable third layer of the [D30] layered locator
//! (task-184, slice 2): locator ID → `(file_id, row_position)`.
//!
//! A shard-local dense array file (`location.arr`, beside `aux.redb`) of fixed
//! **12-byte** entries: `u32 file_id` (LE) + `u64 row_position` (LE). The locator ID
//! *is* the slot index — an entry's byte offset is `id × 12` — so the file needs no
//! keys, no tree, and no per-entry overhead (measured 12.0 B/entry exact vs redb's
//! 53.9 B/entry; see the task-184 plan's spike). `file_id` indexes the
//! interned file table (`files` in `aux.redb`); the row position is the row's ordinal
//! in that Iceberg data file.
//!
//! **Durability / crash contract** (extends the store's two-phase commit): appends and
//! patches are written and **fsynced before** the durable Tantivy commit of the docs
//! that reference them. A crash after the fsync but before the Tantivy commit leaves
//! *orphan* slots — appended entries no live doc points at — which are harmless
//! (unreachable through any doc; reclaimed by a later store compaction) and cost 12 B
//! each. A crash **mid-append** can leave a torn partial entry at the tail; on open the
//! length is floored to whole entries, so the torn bytes sit past `len()` and are
//! overwritten by the next append. Deletes never touch the array — a deleted doc's slot
//! simply becomes unreachable.
//!
//! **Reads are lock-free**: `get` is a positional `pread` (`FileExt::read_at` on
//! `&File`), safe under concurrent appends/patches because entries are fixed-size and a
//! reader only dereferences ids it obtained from a *committed* doc's fast field. We
//! chose pread over an mmap-and-remap strategy deliberately: one syscall per 12-byte
//! lookup is cheap at hydration volumes, and it avoids remap-on-grow lifetime hazards.
//! **Writes** (append/patch/sync) are serialized by the shard's writer lock; a small
//! internal mutex additionally guards the append offset so the store is safe even if a
//! future caller writes outside that lock.
//!
//! [D30]: ../../../okf/system/decisions/d30-layered-locator.md

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Filename of the dense location array, beside `aux.redb` in the shard dir.
pub const LOCATION_FILE: &str = "location.arr";

/// Fixed size of one entry: `u32 file_id` LE + `u64 row_position` LE.
pub const ENTRY_BYTES: u64 = 12;

/// The dense location array: locator ID → `(file_id, row_position)`. See the module
/// docs for the format and the crash/concurrency contract.
pub struct LocationStore {
    file: File,
    /// Number of **complete** entries (torn tail bytes excluded); the next appended
    /// entry gets id `len`. `Acquire`/`Release` pairs `get`'s bound check with the
    /// append that published the entry.
    len: AtomicU64,
    /// Serializes appends (the offset computation + length publish). Patches and reads
    /// don't need it — they address existing slots positionally.
    append: Mutex<()>,
}

impl LocationStore {
    /// Open (or create) the array at `path`. The next locator id is `file_len / 12`;
    /// a torn tail (crash mid-append) is floored away and overwritten by the next append.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let len = file.metadata()?.len() / ENTRY_BYTES;
        Ok(Self {
            file,
            len: AtomicU64::new(len),
            append: Mutex::new(()),
        })
    }

    /// Append `entries` (`(file_id, row_position)` each), returning the id of the
    /// **first** — the rest follow densely. Not durable until [`sync`](Self::sync).
    pub fn append(&self, entries: &[(u32, u64)]) -> io::Result<u64> {
        let _guard = self.append.lock().expect("location append lock");
        let first = self.len.load(Ordering::Relaxed);
        let mut buf = Vec::with_capacity(entries.len() * ENTRY_BYTES as usize);
        for (file_id, pos) in entries {
            buf.extend_from_slice(&file_id.to_le_bytes());
            buf.extend_from_slice(&pos.to_le_bytes());
        }
        self.file.write_all_at(&buf, first * ENTRY_BYTES)?;
        // Publish the new length only after the bytes are written, so a concurrent
        // `get` never reads a slot the write hasn't reached yet.
        self.len
            .store(first + entries.len() as u64, Ordering::Release);
        Ok(first)
    }

    /// Overwrite slot `id` in place with a new `(file_id, row_position)` — the update
    /// path when a key's row moved (upsert reuse now; compaction re-map in slice 3).
    /// Errors on an id past the end (slots are only ever allocated by `append`).
    pub fn patch(&self, id: u64, file_id: u32, row_position: u64) -> io::Result<()> {
        if id >= self.len.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("patch of unallocated location slot {id}"),
            ));
        }
        let mut buf = [0u8; ENTRY_BYTES as usize];
        buf[..4].copy_from_slice(&file_id.to_le_bytes());
        buf[4..].copy_from_slice(&row_position.to_le_bytes());
        self.file.write_all_at(&buf, id * ENTRY_BYTES)
    }

    /// Read slot `id` → `(file_id, row_position)`, or `None` past the end. Lock-free
    /// (`pread` on `&File`) — safe from concurrent search threads.
    pub fn get(&self, id: u64) -> io::Result<Option<(u32, u64)>> {
        if id >= self.len.load(Ordering::Acquire) {
            return Ok(None);
        }
        let mut buf = [0u8; ENTRY_BYTES as usize];
        self.file.read_exact_at(&mut buf, id * ENTRY_BYTES)?;
        let file_id = u32::from_le_bytes(buf[..4].try_into().expect("4 bytes"));
        let pos = u64::from_le_bytes(buf[4..].try_into().expect("8 bytes"));
        Ok(Some((file_id, pos)))
    }

    /// fsync the array — the durability point that must precede the Tantivy commit of
    /// the docs referencing the written slots (the D30 crash contract).
    pub fn sync(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    /// Number of allocated slots; the next appended entry gets this id.
    pub fn len(&self) -> u64 {
        self.len.load(Ordering::Acquire)
    }

    /// Whether the array holds no entries yet.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(dir: &Path) -> LocationStore {
        LocationStore::open(&dir.join(LOCATION_FILE)).unwrap()
    }

    #[test]
    fn append_get_patch_and_len() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        assert_eq!(s.len(), 0);
        assert!(s.is_empty());

        let first = s.append(&[(1, 10), (2, 20)]).unwrap();
        assert_eq!(first, 0);
        assert_eq!(s.len(), 2);
        assert_eq!(s.get(0).unwrap(), Some((1, 10)));
        assert_eq!(s.get(1).unwrap(), Some((2, 20)));

        // ids are dense: the next append continues where the last stopped.
        assert_eq!(s.append(&[(3, 30)]).unwrap(), 2);
        assert_eq!(s.len(), 3);

        // patch rewrites a slot in place without moving anything else.
        s.patch(1, 7, 77).unwrap();
        assert_eq!(s.get(1).unwrap(), Some((7, 77)));
        assert_eq!(s.get(0).unwrap(), Some((1, 10)));
        assert_eq!(s.len(), 3, "patch never grows the array");
    }

    #[test]
    fn get_past_eof_is_none_and_patch_past_eof_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        s.append(&[(1, 1)]).unwrap();
        assert_eq!(s.get(1).unwrap(), None);
        assert_eq!(s.get(u64::MAX).unwrap(), None);
        assert!(s.patch(1, 0, 0).is_err(), "unallocated slot");
    }

    #[test]
    fn entry_byte_layout_is_12_bytes_le_exactly() {
        // Golden test for the on-disk format: 12 B fixed, u32 file_id LE + u64 pos LE,
        // offset = id × 12 (the slice-2 notes' binding decision 1).
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(LOCATION_FILE);
        let s = LocationStore::open(&path).unwrap();
        s.append(&[(0x0403_0201, 0x0C0B_0A09_0807_0605), (1, 2)])
            .unwrap();
        s.sync().unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes.len(), 24, "12 B per entry, exactly");
        assert_eq!(
            &bytes[..12],
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 0x0A, 0x0B, 0x0C],
            "u32 file_id LE then u64 row_position LE"
        );
        assert_eq!(&bytes[12..], &[1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn reopen_persists_entries_and_continues_ids() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let s = store(tmp.path());
            s.append(&[(5, 50), (6, 60)]).unwrap();
            s.sync().unwrap();
        }
        let s = store(tmp.path());
        assert_eq!(s.len(), 2);
        assert_eq!(s.get(0).unwrap(), Some((5, 50)));
        assert_eq!(s.get(1).unwrap(), Some((6, 60)));
        assert_eq!(s.append(&[(7, 70)]).unwrap(), 2, "next id = file_len / 12");
    }

    #[test]
    fn torn_tail_is_floored_on_open_and_overwritten() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(LOCATION_FILE);
        {
            let s = LocationStore::open(&path).unwrap();
            s.append(&[(1, 1)]).unwrap();
            s.sync().unwrap();
        }
        // Simulate a crash mid-append: 5 stray bytes past the last whole entry.
        {
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[0xFF; 5]).unwrap();
        }
        let s = LocationStore::open(&path).unwrap();
        assert_eq!(s.len(), 1, "torn tail excluded");
        assert_eq!(s.get(1).unwrap(), None);
        assert_eq!(s.append(&[(2, 2)]).unwrap(), 1, "torn bytes overwritten");
        assert_eq!(s.get(1).unwrap(), Some((2, 2)));
    }
}
