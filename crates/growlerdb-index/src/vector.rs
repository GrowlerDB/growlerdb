//! Per-segment **ANN (approximate-nearest-neighbor) index** + its on-disk sidecar
//! ([D19](../../../okf/system/decisions/d19-ann-library.md)).
//!
//! A [`VectorIndex`] answers `knn(query, k)` over one segment's stored embeddings for one
//! VECTOR field. Per D19 the ANN artifact is **GrowlerDB-owned, one per segment**, carried
//! through the single Tantivy segment lifecycle: built at segment build, written beside the
//! segment as a versioned sidecar ([`SegmentAnn`]), and backed up / restored with it.
//!
//! **Crate choice.** D19 blesses a pure-Rust HNSW crate (`instant-distance`/`hnsw_rs`) *or* the
//! brute-force exact fallback behind the same trait. This build ships **both** behind
//! [`VectorIndex`] and picks between them by size ([`StoredAnnIndex::build`]):
//! * [`BruteForceIndex`] — brute-force **exact** (no recall loss), the default at small per-segment
//!   N where a full scan is already the fastest correct answer;
//! * [`HnswIndex`] — an approximate **HNSW** graph (via `instant-distance`, MIT/Apache-2.0) chosen
//!   once a segment's vector count for a field crosses [`HNSW_MIN_VECTORS`], where the graph's
//!   sub-linear search pays for itself. It is high-recall (≥0.9 recall@10 on the benchmark set),
//!   not exact.
//!
//! Both express **all three** [`VectorMetric`](growlerdb_core::VectorMetric)s and return the SAME
//! `(docid, score)` shape with the same higher-is-nearer ordering, so callers ([`knn_search`]) are
//! impl-agnostic. `instant-distance` bakes one distance into its point type, so [`HnswIndex`] feeds
//! it `-score(metric, a, b)` as the distance: a strictly-decreasing transform of the similarity, so
//! the graph navigates by the exact same nearest-first comparisons the metric implies (identical to
//! a true Euclidean HNSW for L2/Cosine; Dot is MIPS, well-behaved for the similar-norm vectors we
//! see). The [`SegmentAnn`] sidecar stores each field's index **tagged** ([`StoredAnnIndex`]) so
//! read-back dispatches to the right impl; each index self-describes its `dims`/`metric`.
//!
//! [`knn_search`]: crate::SegmentReader::knn_search

use std::collections::BTreeMap;

use growlerdb_core::VectorMetric;
use instant_distance::{Builder, HnswMap, Point, Search};
use serde::{Deserialize, Serialize};

/// The ANN sidecar's magic tag — mirrors the cold-tier [`sidecar`](crate::sidecar) framing so a
/// wrong-format or pre-versioning file is **detected**, never mis-parsed.
pub const ANN_MAGIC: [u8; 4] = *b"GDBv";
/// Current ANN sidecar format version. Bump on any incompatible payload-layout change.
/// v2: each field's index is a **tagged** [`StoredAnnIndex`] (brute-force vs HNSW) rather than a
/// bare [`BruteForceIndex`].
const ANN_VERSION: u16 = 2;
/// File-name suffix of a segment's ANN sidecar: `<segment-uuid>.ann`, beside the lexical segment.
pub const ANN_SUFFIX: &str = "ann";

/// Per-field vector count at which [`StoredAnnIndex::build`] switches from the exact
/// [`BruteForceIndex`] to the approximate [`HnswIndex`]. Below this an exact full scan is already
/// the fastest correct answer (and has zero recall loss); above it the HNSW graph's sub-linear
/// search wins. 4096 is a deliberately conservative default — "scale is the gate": HNSW's build
/// cost and approximation only earn their keep once a segment holds thousands of vectors for a
/// field. Internal + transparent: both impls answer `knn` identically in shape and ordering.
pub const HNSW_MIN_VECTORS: usize = 4096;

/// HNSW construction/search knobs, fixed so a rebuild of the same vectors is reproducible.
/// `ef_*` trade recall for latency; 100 comfortably clears the ≥0.9 recall@10 bar on the benchmark.
const HNSW_EF_CONSTRUCTION: usize = 100;
const HNSW_EF_SEARCH: usize = 100;
/// A fixed RNG seed so a segment's HNSW graph is a deterministic function of its vectors.
const HNSW_SEED: u64 = 0x6772_6f77_6c65_72db;

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

/// One embedding as an `instant-distance` [`Point`]. Carries its field's `metric` so
/// [`distance`](Point::distance) can express **any** of the three metrics from a single point type,
/// always **smaller = nearer** (what HNSW wants) and always a strictly-decreasing transform of
/// [`score`], so every nearest-first comparison the graph makes matches the metric's own ranking:
/// * `Cosine` — `data` is stored **unit-normalized** (see [`VecPoint::indexed`]), so `dot == cosine`
///   and distance is `-dot`; one dot product per call instead of `score`'s three (`a·b`, `a·a`,
///   `b·b`), which is the difference between a fast HNSW build and a slow one.
/// * `Dot` — distance is `-dot` on the raw vectors (raw inner product).
/// * `L2` — distance is the true Euclidean distance (a proper metric → best HNSW navigation).
///
/// [`HnswIndex::knn`] inverts this back to the exact [`score`] value. The per-point `metric` costs
/// one byte in postcard next to the `dims`-wide `f32` vector — negligible.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct VecPoint {
    metric: VectorMetric,
    data: Vec<f32>,
}

impl VecPoint {
    /// Wrap a raw embedding for **indexing**, unit-normalizing when the metric is `Cosine` so the
    /// stored representation makes `dot == cosine`.
    fn indexed(metric: VectorMetric, v: &[f32]) -> Self {
        let data = match metric {
            VectorMetric::Cosine => normalized(v),
            VectorMetric::Dot | VectorMetric::L2 => v.to_vec(),
        };
        Self { metric, data }
    }

    /// The score a distance from [`distance`](Point::distance) corresponds to — the exact inverse of
    /// the transform above, so it equals [`score`] and matches [`BruteForceIndex`].
    fn score_of(metric: VectorMetric, distance: f32) -> f32 {
        match metric {
            VectorMetric::L2 => 1.0 / (1.0 + distance),
            VectorMetric::Cosine | VectorMetric::Dot => -distance,
        }
    }
}

impl Point for VecPoint {
    fn distance(&self, other: &Self) -> f32 {
        match self.metric {
            VectorMetric::L2 => {
                let d2: f32 = self
                    .data
                    .iter()
                    .zip(other.data.iter())
                    .map(|(x, y)| {
                        let d = x - y;
                        d * d
                    })
                    .sum();
                d2.sqrt()
            }
            // Cosine data is pre-normalized so `dot == cosine`; Dot uses the raw dot. Negate so
            // smaller = nearer.
            VectorMetric::Cosine | VectorMetric::Dot => -dot(&self.data, &other.data),
        }
    }
}

/// The **approximate HNSW** [`VectorIndex`] (`instant-distance`), selected by
/// [`StoredAnnIndex::build`] once a segment's per-field vector count exceeds [`HNSW_MIN_VECTORS`].
/// Search is sub-linear and high-recall (≥0.9 recall@10 on the benchmark), not exact. `knn` returns
/// the SAME `(docid, score)` shape and higher-is-nearer ordering as [`BruteForceIndex`], so it drops
/// in behind the trait with no caller change.
#[derive(Serialize, Deserialize)]
pub struct HnswIndex {
    dims: u32,
    metric: VectorMetric,
    /// Vector count — kept explicit so `len` is `O(1)` and answers even for an empty index.
    len: u32,
    /// The built graph mapping each point to its segment-local docid. `None` only for an empty
    /// build (`instant-distance` needs no graph for zero points; `knn` short-circuits).
    map: Option<HnswMap<VecPoint, u32>>,
}

impl VectorIndex for HnswIndex {
    fn build(dims: usize, metric: VectorMetric, items: &[(u32, Vec<f32>)]) -> Self {
        let len = items.len() as u32;
        if items.is_empty() {
            return Self {
                dims: dims as u32,
                metric,
                len: 0,
                map: None,
            };
        }
        let points: Vec<VecPoint> = items
            .iter()
            .map(|(_, v)| VecPoint::indexed(metric, v))
            .collect();
        let values: Vec<u32> = items.iter().map(|(id, _)| *id).collect();
        let map = Builder::default()
            .seed(HNSW_SEED)
            .ef_construction(HNSW_EF_CONSTRUCTION)
            .ef_search(HNSW_EF_SEARCH)
            .build(points, values);
        Self {
            dims: dims as u32,
            metric,
            len,
            map: Some(map),
        }
    }

    fn knn(&self, query: &[f32], k: usize) -> Vec<(u32, f32)> {
        if k == 0 {
            return Vec::new();
        }
        let Some(map) = self.map.as_ref() else {
            return Vec::new();
        };
        // The query is normalized the same way indexed points are (Cosine), so `dot == cosine`.
        let q = VecPoint::indexed(self.metric, query);
        let mut search = Search::default();
        // `search` yields candidates nearest-first (ascending distance = descending score). Recover
        // the exact `score` from each distance, then re-sort with the SAME descending-score,
        // docid-tiebreak ordering as `BruteForceIndex` so cross-segment/impl merges are
        // deterministic.
        let mut scored: Vec<(u32, f32)> = map
            .search(&q, &mut search)
            .take(k)
            .map(|item| (*item.value, VecPoint::score_of(self.metric, item.distance)))
            .collect();
        scored.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        scored
    }

    fn to_bytes(&self) -> Vec<u8> {
        postcard::to_allocvec(self).unwrap_or_default()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, VectorIndexError> {
        postcard::from_bytes(bytes).map_err(|e| VectorIndexError::Decode(e.to_string()))
    }

    fn dims(&self) -> usize {
        self.dims as usize
    }

    fn len(&self) -> usize {
        self.len as usize
    }
}

/// A **tagged** per-field index in the [`SegmentAnn`] sidecar: either the exact [`BruteForceIndex`]
/// or the approximate [`HnswIndex`]. Postcard writes a leading variant discriminant, so read-back
/// dispatches to the right impl with no external schema. Answers `knn`/`len`/`dims` uniformly so
/// [`knn_search`](crate::SegmentReader::knn_search) never sees which impl it holds.
#[derive(Serialize, Deserialize)]
pub enum StoredAnnIndex {
    /// Exact brute-force scan — the small-N default.
    BruteForce(BruteForceIndex),
    /// Approximate HNSW — selected above [`HNSW_MIN_VECTORS`].
    Hnsw(HnswIndex),
}

impl StoredAnnIndex {
    /// Build the index, **auto-selecting** the impl by size: [`HnswIndex`] once `items` exceeds
    /// [`HNSW_MIN_VECTORS`], else the exact [`BruteForceIndex`]. Selection is internal and
    /// transparent — both answer `knn` with identical shape and ordering.
    pub fn build(dims: usize, metric: VectorMetric, items: &[(u32, Vec<f32>)]) -> Self {
        if items.len() > HNSW_MIN_VECTORS {
            Self::Hnsw(HnswIndex::build(dims, metric, items))
        } else {
            Self::BruteForce(BruteForceIndex::build(dims, metric, items))
        }
    }

    /// The `k` nearest docids to `query`, best-first — dispatched to the concrete impl.
    pub fn knn(&self, query: &[f32], k: usize) -> Vec<(u32, f32)> {
        match self {
            Self::BruteForce(i) => i.knn(query, k),
            Self::Hnsw(i) => i.knn(query, k),
        }
    }

    /// The number of indexed vectors.
    pub fn len(&self) -> usize {
        match self {
            Self::BruteForce(i) => i.len(),
            Self::Hnsw(i) => i.len(),
        }
    }

    /// The embedding dimensionality this index was built for.
    pub fn dims(&self) -> usize {
        match self {
            Self::BruteForce(i) => i.dims(),
            Self::Hnsw(i) => i.dims(),
        }
    }

    /// The field's distance metric (self-described by the stored index). Lets the exact
    /// filtered-KNN path score a subset without an external schema.
    pub fn metric(&self) -> VectorMetric {
        match self {
            Self::BruteForce(i) => i.metric,
            Self::Hnsw(i) => i.metric,
        }
    }

    /// Whether the index holds no vectors.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Serialize to the tagged sidecar payload bytes.
    fn to_bytes(&self) -> Vec<u8> {
        postcard::to_allocvec(self).unwrap_or_default()
    }

    /// Deserialize from [`to_bytes`](Self::to_bytes) output, dispatching on the variant tag.
    fn from_bytes(bytes: &[u8]) -> Result<Self, VectorIndexError> {
        postcard::from_bytes(bytes).map_err(|e| VectorIndexError::Decode(e.to_string()))
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
///
/// `pub(crate)` so the **exact filtered-KNN** path in [`knn_search`](crate::SegmentReader::knn_search)
/// can score a filter-allowed subset directly from stored vectors with the field's metric.
pub(crate) fn score(metric: VectorMetric, a: &[f32], b: &[f32]) -> f32 {
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

/// `v` scaled to unit length (a zero vector is returned unchanged — its cosine to anything is 0,
/// which `dot` then yields, matching [`score`]'s zero-vector handling).
fn normalized(v: &[f32]) -> Vec<f32> {
    let n = dot(v, v).sqrt();
    if n == 0.0 {
        v.to_vec()
    } else {
        v.iter().map(|x| x / n).collect()
    }
}

/// One segment's ANN sidecar: the per-field [`StoredAnnIndex`]es for every VECTOR field that had
/// at least one vector in the segment, keyed by field path. Serialized as a versioned frame
/// (magic + version + postcard) written to `<segment-uuid>.ann` beside the lexical segment and
/// registered in the segment's backup file set.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SegmentAnn {
    /// Field path → that field's **tagged** index bytes ([`StoredAnnIndex::to_bytes`]). Opaque
    /// bytes keep the container agnostic of the concrete [`VectorIndex`] implementation; the tag
    /// inside picks brute-force vs HNSW on read-back.
    fields: BTreeMap<String, Vec<u8>>,
}

impl SegmentAnn {
    /// An empty sidecar.
    pub fn new() -> Self {
        Self::default()
    }

    /// Store `index` under `field`.
    pub fn insert(&mut self, field: String, index: &StoredAnnIndex) {
        self.fields.insert(field, index.to_bytes());
    }

    /// The index for `field`, if the sidecar carries one.
    pub fn field(&self, field: &str) -> Option<StoredAnnIndex> {
        self.fields
            .get(field)
            .and_then(|b| StoredAnnIndex::from_bytes(b).ok())
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
            &StoredAnnIndex::BruteForce(BruteForceIndex::build(3, VectorMetric::Cosine, &items())),
        );
        ann.insert(
            "title_vec".into(),
            &StoredAnnIndex::BruteForce(BruteForceIndex::build(3, VectorMetric::L2, &items())),
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

    // --- HNSW approximate index (TASK-301) ---

    /// Deterministic synthetic vectors from a xorshift64 PRNG (no `rand` dependency), so the
    /// recall/latency benchmark is reproducible.
    fn xorshift(state: &mut u64) -> f32 {
        let mut x = *state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *state = x;
        // Top 24 bits → f32 in [-1, 1).
        ((x >> 40) as f32 / (1u32 << 24) as f32) * 2.0 - 1.0
    }

    fn synth(n: usize, dims: usize, seed: u64) -> Vec<(u32, Vec<f32>)> {
        let mut s = seed | 1;
        (0..n)
            .map(|i| ((i as u32), (0..dims).map(|_| xorshift(&mut s)).collect()))
            .collect()
    }

    fn recall_at_k(exact: &[(u32, f32)], approx: &[(u32, f32)], k: usize) -> f32 {
        let truth: std::collections::HashSet<u32> =
            exact.iter().take(k).map(|(id, _)| *id).collect();
        let hit = approx
            .iter()
            .take(k)
            .filter(|(id, _)| truth.contains(id))
            .count();
        hit as f32 / k as f32
    }

    #[test]
    fn hnsw_returns_same_shape_and_ordering_as_bruteforce() {
        let data = synth(500, 16, 7);
        for metric in [VectorMetric::Cosine, VectorMetric::Dot, VectorMetric::L2] {
            let hnsw = HnswIndex::build(16, metric, &data);
            assert_eq!(hnsw.dims(), 16);
            assert_eq!(hnsw.len(), 500);
            assert!(!hnsw.is_empty());
            let out = hnsw.knn(&data[3].1, 5);
            assert_eq!(out.len(), 5);
            // The query is an indexed point, so it must be its own nearest neighbor.
            assert_eq!(out[0].0, 3);
            // Best-first: scores descend.
            assert!(out.windows(2).all(|w| w[0].1 >= w[1].1));
            assert!(hnsw.knn(&data[0].1, 0).is_empty());
        }
    }

    #[test]
    fn hnsw_index_round_trips() {
        let data = synth(300, 24, 11);
        let idx = HnswIndex::build(24, VectorMetric::Cosine, &data);
        let back = HnswIndex::from_bytes(&idx.to_bytes()).unwrap();
        assert_eq!(back.dims(), 24);
        assert_eq!(back.len(), 300);
        // Identical query results before and after a serialize round-trip.
        let q = &data[42].1;
        assert_eq!(idx.knn(q, 10), back.knn(q, 10));
    }

    #[test]
    fn empty_hnsw_is_well_formed() {
        let idx = HnswIndex::build(8, VectorMetric::L2, &[]);
        assert!(idx.is_empty());
        assert_eq!(idx.len(), 0);
        assert!(idx.knn(&[0.0; 8], 5).is_empty());
        let back = HnswIndex::from_bytes(&idx.to_bytes()).unwrap();
        assert!(back.is_empty());
    }

    #[test]
    fn stored_ann_auto_selects_and_dispatches() {
        // Below the threshold → exact brute-force.
        let small = synth(HNSW_MIN_VECTORS, 8, 1);
        assert!(matches!(
            StoredAnnIndex::build(8, VectorMetric::Cosine, &small),
            StoredAnnIndex::BruteForce(_)
        ));
        // Above the threshold → approximate HNSW.
        let big = synth(HNSW_MIN_VECTORS + 1, 8, 1);
        let idx = StoredAnnIndex::build(8, VectorMetric::Cosine, &big);
        assert!(matches!(idx, StoredAnnIndex::Hnsw(_)));
        assert_eq!(idx.len(), HNSW_MIN_VECTORS + 1);
        assert_eq!(idx.dims(), 8);
        assert_eq!(idx.knn(&big[0].1, 3).len(), 3);
        // Tagged round-trip picks the right impl back.
        let back = StoredAnnIndex::from_bytes(&idx.to_bytes()).unwrap();
        assert!(matches!(back, StoredAnnIndex::Hnsw(_)));
    }

    /// Always-run guardrail: HNSW recall@10 stays high on a moderate synthetic set.
    #[test]
    fn hnsw_recall_at_10_holds() {
        let (n, dims, k) = (2000usize, 64usize, 10usize);
        let data = synth(n, dims, 99);
        let exact = BruteForceIndex::build(dims, VectorMetric::Cosine, &data);
        let hnsw = HnswIndex::build(dims, VectorMetric::Cosine, &data);
        let queries = synth(50, dims, 2024);
        let mut total = 0.0f32;
        for (_, q) in &queries {
            total += recall_at_k(&exact.knn(q, k), &hnsw.knn(q, k), k);
        }
        let recall = total / queries.len() as f32;
        assert!(recall >= 0.9, "recall@{k} = {recall:.4} < 0.90");
    }

    /// AC#2 benchmark: 10k × dims=128, recall@10 vs exact + build/query latency, both impls.
    /// Heavier, so `#[ignore]`d — run with `cargo test -p growlerdb-index -- --ignored --nocapture`
    /// to print the numbers cited in the PR.
    #[test]
    #[ignore = "benchmark: run with --ignored --nocapture to print recall + latency"]
    fn hnsw_recall_and_latency_benchmark() {
        use std::time::Instant;
        let (n, dims, k) = (10_000usize, 128usize, 10usize);
        let metric = VectorMetric::Cosine;
        let data = synth(n, dims, 12345);
        let queries: Vec<Vec<f32>> = synth(200, dims, 777).into_iter().map(|(_, v)| v).collect();

        let t = Instant::now();
        let exact = BruteForceIndex::build(dims, metric, &data);
        let bf_build = t.elapsed();

        let t = Instant::now();
        let hnsw = HnswIndex::build(dims, metric, &data);
        let hnsw_build = t.elapsed();

        let t = Instant::now();
        let bf_results: Vec<_> = queries.iter().map(|q| exact.knn(q, k)).collect();
        let bf_query = t.elapsed();

        let t = Instant::now();
        let hnsw_results: Vec<_> = queries.iter().map(|q| hnsw.knn(q, k)).collect();
        let hnsw_query = t.elapsed();

        let recall = bf_results
            .iter()
            .zip(&hnsw_results)
            .map(|(e, a)| recall_at_k(e, a, k))
            .sum::<f32>()
            / queries.len() as f32;

        let nq = queries.len() as u32;
        println!("=== HNSW ANN benchmark (n={n}, dims={dims}, k={k}, {metric:?}) ===");
        println!("recall@{k}         : {recall:.4}");
        println!("build  brute-force : {bf_build:?}");
        println!("build  hnsw        : {hnsw_build:?}");
        println!(
            "query  brute-force : {bf_query:?} total, {:?}/query",
            bf_query / nq
        );
        println!(
            "query  hnsw        : {hnsw_query:?} total, {:?}/query",
            hnsw_query / nq
        );
        assert!(recall >= 0.9, "recall@{k} = {recall:.4} < 0.90");
    }
}
