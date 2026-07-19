//! The embedding seam: turn text into dense vectors. Local by default (D20);
//! external providers attach via this trait (D41 open-core, retrieval-first D42).

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::api::LocatedDoc;
use crate::doc::{Document, Value};
use crate::index_def::{EmbedProvider, ResolvedIndex, VectorSpec};

/// Errors from producing embeddings.
#[derive(Debug, thiserror::Error)]
pub enum EmbedError {
    /// The embedding backend failed (model load, inference, transport, …).
    #[error("embedding backend error: {0}")]
    Backend(String),
    /// The backend returned a vector whose length didn't match the configured `dims`.
    #[error("embedding dimension mismatch: expected {expected}, got {got}")]
    DimMismatch {
        /// The dimensionality the field is configured for.
        expected: usize,
        /// The dimensionality the backend actually returned.
        got: usize,
    },
}

/// Turns text into dense vectors. The seam a real model (local BGE runtime, or an
/// external service) plugs into; the built-in [`HashEmbedder`] is the default today.
pub trait Embedder: Send + Sync {
    /// The embedding model id (recorded in index metadata for reproducibility).
    fn model_id(&self) -> &str;
    /// Output dimensionality.
    fn dims(&self) -> usize;
    /// Embed a batch of texts; returns one `dims()`-length vector per input, in order.
    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError>;
}

/// Deterministic, dependency-free embedder for tests and as the default until the
/// local BGE runtime lands. Hashes whitespace tokens into a fixed-dim bag, L2-normalized.
/// Stable across runs (no RNG). NOT a semantic model.
pub struct HashEmbedder {
    model_id: String,
    dims: usize,
}

impl HashEmbedder {
    /// Build a hash embedder emitting `dims`-length vectors tagged with `model_id`.
    pub fn new(model_id: impl Into<String>, dims: usize) -> Self {
        Self {
            model_id: model_id.into(),
            dims,
        }
    }
}

impl Embedder for HashEmbedder {
    fn model_id(&self) -> &str {
        &self.model_id
    }

    fn dims(&self) -> usize {
        self.dims
    }

    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        Ok(texts
            .iter()
            .map(|text| {
                let mut v = vec![0f32; self.dims];
                if self.dims == 0 {
                    return v;
                }
                for token in text.split_whitespace() {
                    // A fixed-seed hash (`DefaultHasher` is deterministic across a build) folds
                    // each token into a bucket — a stable bag-of-tokens, never an RNG.
                    let mut hasher = DefaultHasher::new();
                    token.hash(&mut hasher);
                    let bucket = (hasher.finish() % self.dims as u64) as usize;
                    v[bucket] += 1.0;
                }
                // L2-normalize so cosine/dot behave; the zero vector (empty text) stays zeros.
                let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
                if norm > 0.0 {
                    for x in &mut v {
                        *x /= norm;
                    }
                }
                v
            })
            .collect())
    }
}

/// The embedder to use for `spec`. Returns the built-in [`HashEmbedder`] today; the
/// local BGE runtime replaces this body in a follow-on (keyed on `spec.model`/`provider`).
pub fn default_embedder(spec: &VectorSpec) -> Arc<dyn Embedder> {
    Arc::new(HashEmbedder::new(spec.model.clone(), spec.dims))
}

/// Populate every LOCAL vector field's embedding on `docs` in place, batching the embed
/// call per field across the whole slice. A doc missing the source text embeds the empty
/// string (yielding the zero vector), so every doc gets a vector of the field's `dims`.
/// External-provider fields are skipped (unsupported today), as is a field whose backend
/// errors — this is a best-effort ingest transform, not a validation gate.
pub fn embed_vector_fields(idx: &ResolvedIndex, docs: &mut [Document]) {
    let mut refs: Vec<&mut Document> = docs.iter_mut().collect();
    embed_docs(idx, &mut refs);
}

/// [`embed_vector_fields`] over the ingest [`LocatedDoc`] wrapper (the shape the source
/// streams) — embeds the wrapped documents in place.
pub fn embed_located_docs(idx: &ResolvedIndex, docs: &mut [LocatedDoc]) {
    let mut refs: Vec<&mut Document> = docs.iter_mut().map(|l| &mut l.doc).collect();
    embed_docs(idx, &mut refs);
}

/// Shared core: fill in each LOCAL vector field's embedding across `docs`.
fn embed_docs(idx: &ResolvedIndex, docs: &mut [&mut Document]) {
    for f in &idx.fields {
        let Some(spec) = f.vector.as_ref() else {
            continue;
        };
        if spec.provider != EmbedProvider::Local {
            continue;
        }
        let embedder = default_embedder(spec);
        let texts: Vec<String> = docs
            .iter()
            .map(|d| source_text(&d.fields, &spec.source_field))
            .collect();
        let Ok(vectors) = embedder.embed(&texts) else {
            continue;
        };
        for (d, v) in docs.iter_mut().zip(vectors) {
            d.fields.insert(f.path.clone(), Value::Vector(v));
        }
    }
}

/// The text to embed for `source_field`: its indexed-string form, or `""` when the
/// document doesn't carry it.
fn source_text(fields: &BTreeMap<String, Value>, source_field: &str) -> String {
    fields
        .get(source_field)
        .map(Value::to_index_string)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::doc::CompositeKey;
    use crate::index_def::{
        FieldType, ResolvedField, ResolvedKey, Source, VectorMetric, DEFAULT_EMBED_DIMS,
    };

    fn spec(dims: usize) -> VectorSpec {
        VectorSpec {
            dims,
            model: "test-model".into(),
            metric: VectorMetric::Cosine,
            provider: EmbedProvider::Local,
            source_field: "body".into(),
        }
    }

    #[test]
    fn hash_embedder_is_deterministic() {
        let e = HashEmbedder::new("m", 8);
        let a = e.embed(&["hello world foo".into()]).unwrap();
        let b = e.embed(&["hello world foo".into()]).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn hash_embedder_dims_and_batch_length() {
        let e = HashEmbedder::new("m", 16);
        assert_eq!(e.dims(), 16);
        assert_eq!(e.model_id(), "m");
        let out = e
            .embed(&["one".into(), "two words".into(), "three".into()])
            .unwrap();
        assert_eq!(out.len(), 3);
        assert!(out.iter().all(|v| v.len() == 16));
    }

    #[test]
    fn hash_embedder_is_l2_normalized_for_nonempty_text() {
        let e = HashEmbedder::new("m", 32);
        let v = &e.embed(&["alpha beta gamma alpha".into()]).unwrap()[0];
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-5, "norm was {norm}");
    }

    #[test]
    fn hash_embedder_zero_vector_for_empty_text() {
        let e = HashEmbedder::new("m", 8);
        let v = &e.embed(&["".into(), "   ".into()]).unwrap()[0];
        assert!(v.iter().all(|x| *x == 0.0));
    }

    #[test]
    fn default_embedder_matches_spec() {
        let e = default_embedder(&spec(DEFAULT_EMBED_DIMS));
        assert_eq!(e.dims(), DEFAULT_EMBED_DIMS);
        assert_eq!(e.model_id(), "test-model");
    }

    fn vector_index() -> ResolvedIndex {
        ResolvedIndex {
            name: "docs".into(),
            source: Source::Iceberg(crate::index_def::IcebergSource {
                catalog: "c".into(),
                table: "n.t".into(),
                scan: Default::default(),
            }),
            key: ResolvedKey {
                partition_fields: vec![],
                identifier_fields: vec!["id".into()],
            },
            fields: vec![
                ResolvedField {
                    path: "body".into(),
                    ty: FieldType::Text,
                    analyzer: None,
                    format: None,
                    fast: false,
                    indexed: true,
                    record: crate::index_def::TextRecord::Position,
                    fieldnorms: true,
                    cached: false,
                    sensitive: false,
                    max_bytes: None,
                    vector: None,
                },
                ResolvedField {
                    path: "body_vec".into(),
                    ty: FieldType::Vector,
                    analyzer: None,
                    format: None,
                    fast: false,
                    indexed: false,
                    record: crate::index_def::TextRecord::Position,
                    fieldnorms: true,
                    cached: false,
                    sensitive: false,
                    max_bytes: None,
                    vector: Some(spec(8)),
                },
            ],
            equality_deletes: Default::default(),
            warnings: vec![],
            shard_count: 1,
            tenant_field: None,
            windowing: None,
            location_strategy: Default::default(),
        }
    }

    fn doc(id: i64, body: Option<&str>) -> Document {
        let mut fields = BTreeMap::new();
        if let Some(b) = body {
            fields.insert("body".to_string(), Value::Str(b.to_string()));
        }
        Document::new(
            CompositeKey::new(vec![], vec![("id".into(), Value::Int(id))]),
            fields,
        )
    }

    #[test]
    fn embed_vector_fields_populates_vectors() {
        let idx = vector_index();
        let mut docs = vec![doc(1, Some("hello world")), doc(2, None)];
        embed_vector_fields(&idx, &mut docs);
        for d in &docs {
            match d.fields.get("body_vec") {
                Some(Value::Vector(v)) => assert_eq!(v.len(), 8),
                other => panic!("expected a vector, got {other:?}"),
            }
        }
        // The doc missing `body` embeds the empty string → the zero vector.
        let Some(Value::Vector(v)) = docs[1].fields.get("body_vec") else {
            unreachable!()
        };
        assert!(v.iter().all(|x| *x == 0.0));
    }
}
