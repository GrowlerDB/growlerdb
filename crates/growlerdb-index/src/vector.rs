//! Per-segment **ANN (approximate-nearest-neighbor) index** + its on-disk sidecar
//! ([D19](../../../okf/system/decisions/d19-ann-library.md)).
//!
//! A [`VectorIndex`] answers `knn(query, k)` over one segment's stored embeddings for one
//! VECTOR field. Per D19 the ANN artifact is **GrowlerDB-owned, one per segment**, carried
//! through the single Tantivy segment lifecycle: built at segment build, written beside the
//! segment as a versioned sidecar ([`SegmentAnn`]), and backed up / restored with it.
//!
//! **Crate choice.** D19 blesses a pure-Rust HNSW crate (`instant-distance`/`hnsw_rs`) *or* the
//! brute-force exact fallback behind the same trait. This build ships the **brute-force exact**
//! [`BruteForceIndex`]: at the current scale (small per-segment N) it is exact (no recall loss),
//! trivially and stably serializable (postcard), and expresses **all three**
//! [`VectorMetric`](growlerdb_core::VectorMetric)s from a single stored representation — where an
//! HNSW crate bakes one distance into its point type and needs its `serde` feature. It adds **no
//! new dependency** (so no `deny.toml` / supply-chain change). An HNSW implementation can later
//! replace it behind the [`VectorIndex`] trait with no change to the sidecar callers — the sidecar
//! stores each field's index as opaque bytes, and each index self-describes its `dims`/`metric`.

use std::collections::BTreeMap;

use growlerdb_core::VectorMetric;
use serde::{Deserialize, Serialize};

/// The ANN sidecar's magic tag — mirrors the cold-tier [`sidecar`](crate::sidecar) framing so a
/// wrong-format or pre-versioning file is **detected**, never mis-parsed.
pub const ANN_MAGIC: [u8; 4] = *b"GDBv";
/// Current ANN sidecar format version. Bump on any incompatible payload-layout change.
const ANN_VERSION: u16 = 1;
/// File-name suffix of a segment's ANN sidecar: `<segment-uuid>.ann`, beside the lexical segment.
pub const ANN_SUFFIX: &str = "ann";

/// Errors from (de)serializing a [`VectorIndex`] or a [`SegmentAnn`] sidecar.
#[derive(Debug, thiserror::Error)]
pub enum VectorIndexError {
    /// The bytes weren't a well-formed sidecar payload (postcard decode failed).
    #[error("ANN sidecar decode: {0}")]
    Decode(String),
    /// The framed sidecar had a wrong magic tag or an unsupported version.
    #[error("unrecognized ANN sidecar (bad magic / unsupported version)")]
    BadFrame,
}

/// A per-segment, per-field approximate-nearest-neighbor index over embeddings.
///
/// `build` takes each doc's **segment-local docid** paired with its embedding; `knn` returns the
/// `k` nearest docids with a **higher-is-nearer** score (so callers rank by descending score, the
/// same convention as BM25 hits). The index (de)serializes to the sidecar payload and
/// self-describes its `dims`/`metric`, so the query path needs no external schema to read it.
pub trait VectorIndex: Sized {
    /// Build an index over `items` — `(segment-local docid, embedding)` — for a field of the given
    /// `dims` and distance `metric`.
    fn build(dims: usize, metric: VectorMetric, items: &[(u32, Vec<f32>)]) -> Self;
    /// The `k` nearest docids to `query`, **best (nearest) first**, each with its similarity
    /// score (higher = nearer). Fewer than `k` when the index holds fewer items.
    fn knn(&self, query: &[f32], k: usize) -> Vec<(u32, f32)>;
    /// Serialize to the sidecar payload bytes.
    fn to_bytes(&self) -> Vec<u8>;
    /// Deserialize from [`to_bytes`](Self::to_bytes) output.
    fn from_bytes(bytes: &[u8]) -> Result<Self, VectorIndexError>;
    /// The embedding dimensionality this index was built for.
    fn dims(&self) -> usize;
    /// The number of indexed vectors.
    fn len(&self) -> usize;
    /// Whether the index holds no vectors.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The **brute-force exact** [`VectorIndex`] (D19 fallback): every `knn` scans all stored vectors
/// and exactly ranks them by the field's metric. Correct (no approximation) at the current
/// per-segment scale.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BruteForceIndex {
    dims: u32,
    metric: VectorMetric,
    /// `(segment-local docid, embedding)` pairs, in build order.
    items: Vec<(u32, Vec<f32>)>,
}

impl VectorIndex for BruteForceIndex {
    fn build(dims: usize, metric: VectorMetric, items: &[(u32, Vec<f32>)]) -> Self {
        Self {
            dims: dims as u32,
            metric,
            items: items.to_vec(),
        }
    }

    fn knn(&self, query: &[f32], k: usize) -> Vec<(u32, f32)> {
        if k == 0 {
            return Vec::new();
        }
        let mut scored: Vec<(u32, f32)> = self
            .items
            .iter()
            .map(|(id, v)| (*id, score(self.metric, query, v)))
            .collect();
        // Descending score (nearest first); NaN sinks to the bottom via a total-ish compare, and
        // the docid is a stable tiebreaker so ties don't reorder nondeterministically.
        scored.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        scored.truncate(k);
        scored
    }

    fn to_bytes(&self) -> Vec<u8> {
        // Infallible in practice (a plain struct of primitives); an alloc failure is the only
        // path, which would abort anyway — so an empty payload is a safe, detectable degrade.
        postcard::to_allocvec(self).unwrap_or_default()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, VectorIndexError> {
        postcard::from_bytes(bytes).map_err(|e| VectorIndexError::Decode(e.to_string()))
    }

    fn dims(&self) -> usize {
        self.dims as usize
    }

    fn len(&self) -> usize {
        self.items.len()
    }
}

/// A **higher-is-nearer** similarity score between `a` and `b` under `metric`:
/// * `Cosine` → cosine similarity in `[-1, 1]` (0 if either vector is the zero vector);
/// * `Dot` → raw inner product;
/// * `L2` → `1 / (1 + euclidean_distance)` in `(0, 1]` — monotonically decreasing in distance, so
///   the ranking is identical to sorting by ascending L2 distance while staying higher-is-better
///   (uniform with the other metrics and with BM25 hit scores).
///
/// Vectors are compared element-wise over their common prefix, so a length mismatch degrades
/// gracefully rather than panicking (the query path validates `dims` upstream).
fn score(metric: VectorMetric, a: &[f32], b: &[f32]) -> f32 {
    match metric {
        VectorMetric::Dot => dot(a, b),
        VectorMetric::Cosine => {
            let na = dot(a, a).sqrt();
            let nb = dot(b, b).sqrt();
            if na == 0.0 || nb == 0.0 {
                0.0
            } else {
                dot(a, b) / (na * nb)
            }
        }
        VectorMetric::L2 => {
            let d2: f32 = a
                .iter()
                .zip(b.iter())
                .map(|(x, y)| {
                    let d = x - y;
                    d * d
                })
                .sum();
            1.0 / (1.0 + d2.sqrt())
        }
    }
}

/// Inner product over the common prefix of `a` and `b`.
fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

/// One segment's ANN sidecar: the per-field [`BruteForceIndex`]es for every VECTOR field that had
/// at least one vector in the segment, keyed by field path. Serialized as a versioned frame
/// (magic + version + postcard) written to `<segment-uuid>.ann` beside the lexical segment and
/// registered in the segment's backup file set.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SegmentAnn {
    /// Field path → that field's index bytes ([`BruteForceIndex::to_bytes`]). Opaque bytes keep
    /// the container agnostic of the concrete [`VectorIndex`] implementation.
    fields: BTreeMap<String, Vec<u8>>,
}

impl SegmentAnn {
    /// An empty sidecar.
    pub fn new() -> Self {
        Self::default()
    }

    /// Store `index` under `field`.
    pub fn insert(&mut self, field: String, index: &BruteForceIndex) {
        self.fields.insert(field, index.to_bytes());
    }

    /// The index for `field`, if the sidecar carries one.
    pub fn field(&self, field: &str) -> Option<BruteForceIndex> {
        self.fields
            .get(field)
            .and_then(|b| BruteForceIndex::from_bytes(b).ok())
    }

    /// Whether the sidecar holds no field indexes (so it need not be written to disk).
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Serialize to the framed on-disk bytes: `magic · version(LE u16) · postcard(fields)`.
    pub fn to_frame(&self) -> Vec<u8> {
        let payload = postcard::to_allocvec(&self.fields).unwrap_or_default();
        let mut out = Vec::with_capacity(6 + payload.len());
        out.extend_from_slice(&ANN_MAGIC);
        out.extend_from_slice(&ANN_VERSION.to_le_bytes());
        out.extend_from_slice(&payload);
        out
    }

    /// Parse a [`to_frame`](Self::to_frame) sidecar — verifying the magic + version and
    /// erroring (never mis-parsing) on a wrong tag or an unsupported version.
    pub fn from_frame(bytes: &[u8]) -> Result<Self, VectorIndexError> {
        if bytes.len() < 6 || bytes[..4] != ANN_MAGIC {
            return Err(VectorIndexError::BadFrame);
        }
        let ver = u16::from_le_bytes([bytes[4], bytes[5]]);
        if ver != ANN_VERSION {
            return Err(VectorIndexError::BadFrame);
        }
        let fields: BTreeMap<String, Vec<u8>> = postcard::from_bytes(&bytes[6..])
            .map_err(|e| VectorIndexError::Decode(e.to_string()))?;
        Ok(Self { fields })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn items() -> Vec<(u32, Vec<f32>)> {
        vec![
            (0, vec![1.0, 0.0, 0.0]),
            (1, vec![0.0, 1.0, 0.0]),
            (2, vec![0.9, 0.1, 0.0]),
            (3, vec![0.0, 0.0, 1.0]),
        ]
    }

    #[test]
    fn cosine_returns_truly_nearest_in_order() {
        let idx = BruteForceIndex::build(3, VectorMetric::Cosine, &items());
        // A query along +x is nearest doc 0 (exact), then doc 2 (mostly +x).
        let out = idx.knn(&[1.0, 0.0, 0.0], 2);
        assert_eq!(
            out.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![0, 2]
        );
        // Best-first: descending score.
        assert!(out[0].1 >= out[1].1);
    }

    #[test]
    fn dot_ranks_by_inner_product() {
        let idx = BruteForceIndex::build(3, VectorMetric::Dot, &items());
        let out = idx.knn(&[2.0, 0.0, 0.0], 3);
        // dots: doc0=2.0, doc2=1.8, doc1=0.0/doc3=0.0 → 0 then 2 then a zero-dot doc.
        assert_eq!(out[0].0, 0);
        assert_eq!(out[1].0, 2);
    }

    #[test]
    fn l2_ranks_by_euclidean_distance() {
        let idx = BruteForceIndex::build(3, VectorMetric::L2, &items());
        let out = idx.knn(&[0.0, 0.0, 0.9], 2);
        // Nearest to (0,0,0.9) is doc 3 = (0,0,1), then whichever is next-closest.
        assert_eq!(out[0].0, 3);
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn knn_respects_k_and_zero_k() {
        let idx = BruteForceIndex::build(3, VectorMetric::Cosine, &items());
        assert_eq!(idx.knn(&[1.0, 0.0, 0.0], 0).len(), 0);
        assert_eq!(idx.knn(&[1.0, 0.0, 0.0], 100).len(), 4); // capped at index size
        assert_eq!(idx.dims(), 3);
        assert_eq!(idx.len(), 4);
        assert!(!idx.is_empty());
    }

    #[test]
    fn index_bytes_round_trip() {
        let idx = BruteForceIndex::build(3, VectorMetric::Cosine, &items());
        let back = BruteForceIndex::from_bytes(&idx.to_bytes()).unwrap();
        assert_eq!(idx, back);
    }

    #[test]
    fn sidecar_frame_round_trips_multiple_fields() {
        let mut ann = SegmentAnn::new();
        ann.insert(
            "body_vec".into(),
            &BruteForceIndex::build(3, VectorMetric::Cosine, &items()),
        );
        ann.insert(
            "title_vec".into(),
            &BruteForceIndex::build(3, VectorMetric::L2, &items()),
        );
        let frame = ann.to_frame();
        let back = SegmentAnn::from_frame(&frame).unwrap();
        assert_eq!(ann, back);
        let body = back.field("body_vec").unwrap();
        assert_eq!(body.knn(&[1.0, 0.0, 0.0], 1)[0].0, 0);
        assert!(back.field("missing").is_none());
    }

    #[test]
    fn bad_frame_is_detected_not_misparsed() {
        assert!(matches!(
            SegmentAnn::from_frame(b"nope"),
            Err(VectorIndexError::BadFrame)
        ));
        // Right magic, wrong version.
        let mut bytes = ANN_MAGIC.to_vec();
        bytes.extend_from_slice(&99u16.to_le_bytes());
        assert!(matches!(
            SegmentAnn::from_frame(&bytes),
            Err(VectorIndexError::BadFrame)
        ));
    }
}
