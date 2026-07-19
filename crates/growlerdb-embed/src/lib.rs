//! GrowlerDB's local embedding runtime.
//!
//! [`embedder_for`] is the single entry point ingest uses to obtain an [`Embedder`] for a
//! vector field. When the `bge` feature is on (the default), a real **bge-small-en-v1.5**
//! BERT model is loaded from a local directory via [Candle](https://github.com/huggingface/candle)
//! — pure Rust, no native/C dependencies, no network. If the model isn't present (or the
//! `bge` feature is disabled), it transparently falls back to core's dependency-free
//! [`HashEmbedder`] so ingest and CI keep working offline.
//!
//! # Model provisioning
//!
//! [`BgeEmbedder`] loads three files from a local directory: `config.json`, `tokenizer.json`,
//! and `model.safetensors`. The directory is resolved as:
//!
//! ```text
//! ${GROWLERDB_MODEL_DIR:-~/.cache/growlerdb/models}/<model-id>/
//! ```
//!
//! e.g. with the default model that is `~/.cache/growlerdb/models/bge-small-en-v1.5/`.
//! Auto-download (via `hf-hub`) is intentionally **not** part of this runtime — provisioning
//! is out of band, which keeps the default build offline and the dependency tree small.

use std::sync::Arc;

use growlerdb_core::index_def::{EmbedProvider, ResolvedIndex, VectorSpec};
use growlerdb_core::{Document, Embedder, HashEmbedder, HashReranker, Reranker, Value};

#[cfg(feature = "bge")]
mod bge;
#[cfg(feature = "bge")]
pub use bge::BgeEmbedder;

#[cfg(feature = "rerank")]
mod bge_rerank;
#[cfg(feature = "rerank")]
pub use bge_rerank::BgeReranker;

/// Default cross-encoder reranker model id ([D21]'s suggested local model). The reranker is
/// opt-in per query and configured with no per-index model today, so the factory targets this
/// one model directory; the [`HashReranker`] fallback carries the same id.
///
/// [D21]: ../../okf/system/decisions/d21-reranker.md
pub const DEFAULT_RERANK_MODEL: &str = "bge-reranker-base";

/// The reranker to use for `model_id`. Returns a real [`BgeReranker`] when the `rerank` feature is
/// enabled and the cross-encoder model loads from the resolved model directory; otherwise falls
/// back to core's dependency-free [`HashReranker`] (token overlap), logging a one-time warning.
/// This is the single factory the search path calls when a query opts into reranking.
pub fn reranker_for(model_id: &str) -> Arc<dyn Reranker> {
    #[cfg(feature = "rerank")]
    match BgeReranker::load(model_id) {
        Ok(r) => return Arc::new(r),
        Err(err) => warn_rerank_fallback(&format!(
            "reranker model unavailable ({err}); using the dev token-overlap reranker. \
             Provision the model under {} (or set GROWLERDB_MODEL_DIR).",
            bge_rerank::model_dir(model_id).display()
        )),
    }

    #[cfg(not(feature = "rerank"))]
    warn_rerank_fallback("the `rerank` feature is disabled; using the dev token-overlap reranker");

    Arc::new(HashReranker::new(model_id))
}

/// Log the reranker fallback reason exactly once per process (repeated per-query rerank calls
/// would otherwise spam it).
fn warn_rerank_fallback(msg: &str) {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| tracing::warn!("{msg}"));
}

/// The embedder to use for `spec`. Returns a real [`BgeEmbedder`] when the `bge` feature is
/// enabled, the provider is [`Local`](EmbedProvider::Local), and the model loads from the
/// resolved model directory; otherwise falls back to core's [`HashEmbedder`] (logging a
/// one-time warning with the model-dir hint). This is the single factory ingest calls.
pub fn embedder_for(spec: &VectorSpec) -> Arc<dyn Embedder> {
    #[cfg(feature = "bge")]
    if spec.provider == EmbedProvider::Local {
        match BgeEmbedder::load(spec) {
            Ok(e) => return Arc::new(e),
            Err(err) => warn_fallback(&format!(
                "BGE model unavailable ({err}); using the dev hash embedder. \
                 Provision the model under {} (or set GROWLERDB_MODEL_DIR).",
                bge::model_dir(&spec.model).display()
            )),
        }
    }

    // Feature off, external provider, or load failure → the deterministic dev embedder.
    #[cfg(not(feature = "bge"))]
    let _ = EmbedProvider::Local; // silence unused import without the feature
    #[cfg(not(feature = "bge"))]
    warn_fallback("the `bge` feature is disabled; using the dev hash embedder");

    Arc::new(HashEmbedder::new(spec.model.clone(), spec.dims))
}

/// Log the fallback-to-hash-embedder reason exactly once per process (repeated per-field
/// ingest calls would otherwise spam it).
fn warn_fallback(msg: &str) {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| tracing::warn!("{msg}"));
}

/// Populate every LOCAL vector field's embedding on `docs` in place, resolving the embedder
/// via [`embedder_for`] (BGE-or-fallback). Mirrors `growlerdb_core::embed_vector_fields` but
/// keyed on this crate's factory rather than core's built-in one.
pub fn embed_vector_fields(idx: &ResolvedIndex, docs: &mut [Document]) {
    let mut refs: Vec<&mut Document> = docs.iter_mut().collect();
    embed_docs(idx, &mut refs);
}

/// [`embed_vector_fields`] over the ingest [`LocatedDoc`](growlerdb_core::LocatedDoc) wrapper —
/// the shape the source streams into the engine.
pub fn embed_located_docs(idx: &ResolvedIndex, docs: &mut [growlerdb_core::LocatedDoc]) {
    let mut refs: Vec<&mut Document> = docs.iter_mut().map(|l| &mut l.doc).collect();
    embed_docs(idx, &mut refs);
}

/// Shared core: fill in each LOCAL vector field's embedding across `docs`, batching the embed
/// call per field. Best-effort (a missing source text embeds `""`; a backend error skips the
/// field) — identical semantics to core's orchestration, only the factory differs.
fn embed_docs(idx: &ResolvedIndex, docs: &mut [&mut Document]) {
    for f in &idx.fields {
        let Some(spec) = f.vector.as_ref() else {
            continue;
        };
        if spec.provider != EmbedProvider::Local {
            continue;
        }
        let embedder = embedder_for(spec);
        let texts: Vec<String> = docs
            .iter()
            .map(|d| source_text(d, &spec.source_field))
            .collect();
        let Ok(vectors) = embedder.embed(&texts) else {
            continue;
        };
        for (d, v) in docs.iter_mut().zip(vectors) {
            d.fields.insert(f.path.clone(), Value::Vector(v));
        }
    }
}

/// The text to embed for `source_field`: its indexed-string form, or `""` when absent.
fn source_text(doc: &Document, source_field: &str) -> String {
    doc.fields
        .get(source_field)
        .map(Value::to_index_string)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use growlerdb_core::index_def::VectorMetric;

    fn spec(dims: usize) -> VectorSpec {
        VectorSpec {
            dims,
            model: "bge-small-en-v1.5".into(),
            metric: VectorMetric::Cosine,
            provider: EmbedProvider::Local,
            source_field: "body".into(),
        }
    }

    #[test]
    fn embedder_for_falls_back_to_hash_when_no_model() {
        // No GROWLERDB_MODEL_DIR provisioned in CI → the hash embedder. This is the CI path
        // for both `bge`-on (load fails, falls back) and `bge`-off (feature gate) builds.
        // Point the model dir at an empty temp dir so a developer's real ~/.cache model can't
        // make this test flake.
        let tmp = tempfile::tempdir().unwrap();
        std::env::set_var("GROWLERDB_MODEL_DIR", tmp.path());

        let e = embedder_for(&spec(384));
        assert_eq!(e.model_id(), "bge-small-en-v1.5");
        assert_eq!(e.dims(), 384);
        let out = e.embed(&["hello world".into(), "".into()]).unwrap();
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|v| v.len() == 384));

        std::env::remove_var("GROWLERDB_MODEL_DIR");
    }

    #[test]
    fn reranker_for_falls_back_to_hash_when_no_model() {
        // No provisioned model dir → the deterministic token-overlap reranker, for both the
        // `rerank`-on (load fails → fallback) and `rerank`-off (feature gate) builds.
        let tmp = tempfile::tempdir().unwrap();
        std::env::set_var("GROWLERDB_MODEL_DIR", tmp.path());

        let r = reranker_for(DEFAULT_RERANK_MODEL);
        assert_eq!(r.model_id(), DEFAULT_RERANK_MODEL);
        // It reorders a known set by token overlap (the fallback's signal), best-first.
        let docs = vec![
            "unrelated words here".to_string(),
            "vector semantic embeddings retrieval".to_string(),
        ];
        let order = r.rerank("semantic vector embeddings", &docs, 2).unwrap();
        assert_eq!(order[0].0, 1, "the overlapping doc reranks first");

        std::env::remove_var("GROWLERDB_MODEL_DIR");
    }
}
