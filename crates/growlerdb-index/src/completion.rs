//! Per-segment **prefix-completion sidecar** — a precomputed top-K-by-doc-frequency table for the
//! `suggest` fields, so `/v1/suggest` answers a short prefix from a `<segment-uuid>.cmp` lookup instead of a live term-dict scan. Lifecycle mirrors [`SegmentAnn`](crate::vector): written after commit, rebuilt on compaction.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// Sidecar file suffix: `<segment-uuid>.cmp`.
pub const COMPLETION_SUFFIX: &str = "cmp";
/// Prefix depth (P): the table covers byte-prefixes of length `1..=P`. A query prefix longer than
/// this falls back to the live seek (fewer matches → already cheap).
pub const COMPLETION_PREFIX_DEPTH: usize = 8;
/// Retained candidates per prefix (K): the K highest-frequency terms under a prefix. Comfortably
/// above the typical `limit` so a cross-segment merge still resolves the global top-`limit`.
pub const COMPLETION_TOP_K: usize = 32;

const CMP_MAGIC: [u8; 4] = *b"GDBc";
const CMP_VERSION: u16 = 1;
/// Compact a prefix's candidate list once it grows past this, bounding build memory for a broad
/// prefix over a high-cardinality field. A multiple of `K` so compaction is infrequent.
const COMPACT_AT: usize = COMPLETION_TOP_K * 8;

/// One field's table: byte-prefix → its top-K `(term, doc_freq)`, each list already ordered by the
/// suggest contract (doc_freq descending, then term ascending).
pub type FieldCompletion = BTreeMap<Vec<u8>, Vec<(String, u64)>>;

/// One segment's completion sidecar: the per-field [`FieldCompletion`] tables, keyed by field path.
/// Serialized as `magic · version(LE u16) · postcard(fields)`, mirroring [`SegmentAnn`](crate::vector).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentCompletion {
    fields: BTreeMap<String, FieldCompletion>,
}

impl SegmentCompletion {
    pub fn new() -> Self {
        Self::default()
    }

    /// Store `table` under `field`.
    pub fn insert(&mut self, field: String, table: FieldCompletion) {
        self.fields.insert(field, table);
    }

    /// The table for `field`, if the sidecar carries one.
    pub fn field(&self, field: &str) -> Option<&FieldCompletion> {
        self.fields.get(field)
    }

    /// Whether the sidecar holds no field tables (so it need not be written to disk).
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Serialize to the framed on-disk bytes: `magic · version(LE u16) · postcard(fields)`.
    pub fn to_frame(&self) -> Vec<u8> {
        let payload = postcard::to_allocvec(&self.fields).unwrap_or_default();
        let mut out = Vec::with_capacity(6 + payload.len());
        out.extend_from_slice(&CMP_MAGIC);
        out.extend_from_slice(&CMP_VERSION.to_le_bytes());
        out.extend_from_slice(&payload);
        out
    }

    /// Parse a [`to_frame`](Self::to_frame) sidecar — verifying the magic + version and erroring
    /// (never mis-parsing) on a wrong tag or an unsupported version.
    pub fn from_frame(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 6 || bytes[..4] != CMP_MAGIC {
            return Err("bad completion sidecar frame".into());
        }
        let ver = u16::from_le_bytes([bytes[4], bytes[5]]);
        if ver != CMP_VERSION {
            return Err(format!("unsupported completion sidecar version {ver}"));
        }
        let fields: BTreeMap<String, FieldCompletion> =
            postcard::from_bytes(&bytes[6..]).map_err(|e| e.to_string())?;
        Ok(Self { fields })
    }
}

/// Accumulates a single field's [`FieldCompletion`] from a term stream: each term feeds its
/// byte-prefixes up to [`COMPLETION_PREFIX_DEPTH`], each keeping only its top-[`COMPLETION_TOP_K`]. Byte-prefixes (not char) match the FST's byte-sorted `starts_with`.
#[derive(Default)]
pub struct CompletionBuilder {
    prefixes: HashMap<Vec<u8>, Vec<(String, u64)>>,
}

impl CompletionBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold `term` (its doc frequency `freq`) into every byte-prefix it belongs to.
    pub fn add(&mut self, term: &[u8], freq: u64) {
        let depth = term.len().min(COMPLETION_PREFIX_DEPTH);
        let term_str = String::from_utf8_lossy(term).into_owned();
        for len in 1..=depth {
            let entry = self.prefixes.entry(term[..len].to_vec()).or_default();
            entry.push((term_str.clone(), freq));
            if entry.len() >= COMPACT_AT {
                trim_top_k(entry);
            }
        }
    }

    /// Finalize each prefix to its ordered top-K, dropping empties.
    pub fn finish(mut self) -> FieldCompletion {
        self.prefixes
            .iter_mut()
            .map(|(prefix, entries)| {
                trim_top_k(entries);
                (prefix.clone(), std::mem::take(entries))
            })
            .filter(|(_, e)| !e.is_empty())
            .collect()
    }
}

/// Sort by the suggest contract (doc_freq descending, then term ascending) and keep the top-K.
fn trim_top_k(entries: &mut Vec<(String, u64)>) {
    entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    entries.truncate(COMPLETION_TOP_K);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_keeps_top_k_by_freq_then_term_per_prefix() {
        let mut b = CompletionBuilder::new();
        b.add(b"berlin", 2);
        b.add(b"bern", 1);
        b.add(b"boston", 4);
        let table = b.finish();
        // "b" spans all three, ranked by freq desc then term asc.
        assert_eq!(
            table.get(b"b".as_slice()).unwrap(),
            &vec![
                ("boston".to_string(), 4),
                ("berlin".to_string(), 2),
                ("bern".to_string(), 1),
            ]
        );
        // "ber" spans only berlin/bern.
        assert_eq!(
            table.get(b"ber".as_slice()).unwrap(),
            &vec![("berlin".to_string(), 2), ("bern".to_string(), 1)]
        );
        // No prefix longer than the term's length.
        assert!(!table.contains_key(b"bostons".as_slice()));
    }

    #[test]
    fn frame_round_trips() {
        let mut b = CompletionBuilder::new();
        b.add(b"alpha", 3);
        let mut sc = SegmentCompletion::new();
        sc.insert("f".into(), b.finish());
        let back = SegmentCompletion::from_frame(&sc.to_frame()).unwrap();
        assert_eq!(sc, back);
        assert_eq!(
            back.field("f").unwrap().get(b"al".as_slice()).unwrap(),
            &vec![("alpha".to_string(), 3)]
        );
    }

    #[test]
    fn bad_frame_is_detected() {
        assert!(SegmentCompletion::from_frame(b"nope").is_err());
        let mut bytes = CMP_MAGIC.to_vec();
        bytes.extend_from_slice(&99u16.to_le_bytes());
        assert!(SegmentCompletion::from_frame(&bytes).is_err());
    }

    #[test]
    fn compaction_preserves_the_true_top_k() {
        // Feed far more than K under one prefix; the highest-freq K must survive compaction.
        let mut b = CompletionBuilder::new();
        for i in 0..(COMPACT_AT * 2) {
            b.add(format!("x{i:05}").as_bytes(), i as u64);
        }
        let table = b.finish();
        let top = table.get(b"x".as_slice()).unwrap();
        assert_eq!(top.len(), COMPLETION_TOP_K);
        // The single highest-frequency term is the last one added.
        assert_eq!(top[0].1, (COMPACT_AT * 2 - 1) as u64);
    }
}
