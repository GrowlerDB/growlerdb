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
use growlerdb_core::{Document, EmbedError, Embedder, HashEmbedder, HashReranker, Reranker, Value};

/// Serialize every test that mutates a process-global env var this crate reads
/// (`GROWLERDB_MODEL_DIR`, `GROWLERDB_*_API_KEY`, `GROWLERDB_*_ENDPOINT`,
/// `GROWLERDB_RERANK_PROVIDER`). `set_var`/`remove_var` are process-wide, so tests across modules
/// race under `cargo test`'s parallelism unless they share ONE lock. Every such test takes this guard.
#[cfg(test)]
pub(crate) fn env_guard() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    let guard = LOCK
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    // Drop the provider-key TTL cache so this test sees the env it's about to set, not a value a
    // prior serialized test cached.
    crate::secrets::clear_key_cache();
    guard
}

#[cfg(feature = "bge")]
mod bge;
#[cfg(feature = "bge")]
pub use bge::BgeEmbedder;

/// pyke's prebuilt ONNX Runtime static lib (built with an older GCC) references
/// `__cxa_call_terminate`, a libstdc++ internal exception-handling symbol that GCC 12
/// (Debian bookworm's toolchain, our release-image builder) **removed** — so the static link can't
/// resolve it. It is only ever reached when a C++ exception escapes a `noexcept` boundary, an
/// already-fatal path, so `abort()` is a sound stand-in. Linux-only: macOS (libc++) resolves
/// exception handling natively and needs no shim. Gated on `bge` (the only feature that links ort).
#[cfg(all(feature = "bge", target_os = "linux"))]
#[no_mangle]
pub extern "C" fn __cxa_call_terminate(_exception: *mut core::ffi::c_void) {
    std::process::abort();
}

#[cfg(feature = "rerank")]
mod bge_rerank;
#[cfg(feature = "rerank")]
pub use bge_rerank::BgeReranker;

#[cfg(feature = "external")]
mod external;
#[cfg(feature = "external")]
pub use external::{ExternalEmbedder, ExternalReranker};

mod secrets;
pub use secrets::{redact, ProviderSecrets};

/// Default cross-encoder reranker model id ([D21]'s suggested local model). The reranker is
/// opt-in per query and configured with no per-index model today, so the factory targets this
/// one model directory; the [`HashReranker`] fallback carries the same id.
///
/// [D21]: ../../../okf/system/decisions/d21-reranker.md
pub const DEFAULT_RERANK_MODEL: &str = "bge-reranker-base";

/// The reranker to use for `model_id`. Returns a real [`BgeReranker`] when the `rerank` feature is
/// enabled and the cross-encoder model loads from the resolved model directory; otherwise falls
/// back to core's dependency-free [`HashReranker`] (token overlap), logging a one-time warning.
/// This is the single factory the search path calls when a query opts into reranking.
pub fn reranker_for(model_id: &str) -> Arc<dyn Reranker> {
    // Opt-in external provider (GROWLERDB_RERANK_PROVIDER=external): call a hosted reranker over
    // HTTP with a server-side-only key. Fail closed (no key ⇒ error at `rerank()`), never a silent
    // fall back to the dev reranker.
    #[cfg(feature = "external")]
    if external::rerank_provider_is_external() {
        return Arc::new(external::ExternalReranker::from_env(model_id));
    }

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
    match spec.provider {
        EmbedProvider::Local => local_embedder(spec),
        EmbedProvider::External => external_embedder(spec),
    }
}

/// The LOCAL, keyless embedder: a real [`BgeEmbedder`] when provisioned, else the dev
/// [`HashEmbedder`]. Never reads a provider secret.
///
/// Loaded models are **cached per resolved model directory** for the life of the process:
/// the factory is called on every semantic query (and every ingest batch), and a BGE load is a
/// 133 MB safetensors read + graph build — per-call loading made every query pay seconds and
/// turned memory pressure into silent hash-fallback queries against real-model document vectors.
/// Only successful loads are cached; a missing model keeps probing (cheap) so provisioning the
/// model directory doesn't require a restart.
fn local_embedder(spec: &VectorSpec) -> Arc<dyn Embedder> {
    #[cfg(feature = "bge")]
    {
        use std::collections::HashMap;
        use std::path::PathBuf;
        use std::sync::{Mutex, OnceLock};
        /// Cache key: (resolved model dir, dims) — the inputs that change which model loads.
        type BgeCache = Mutex<HashMap<(PathBuf, usize), Arc<dyn Embedder>>>;
        static BGE_CACHE: OnceLock<BgeCache> = OnceLock::new();
        let key = (bge::model_dir(&spec.model), spec.dims);
        // Hold the lock across the load: a second concurrent caller waits instead of
        // double-loading 133 MB. First-load-only cost; every later call is a map hit.
        let mut cache = BGE_CACHE
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if let Some(e) = cache.get(&key) {
            return e.clone();
        }
        match BgeEmbedder::load(spec) {
            Ok(e) => {
                let e: Arc<dyn Embedder> = Arc::new(e);
                cache.insert(key, e.clone());
                return e;
            }
            Err(err) => warn_fallback(&format!(
                "BGE model unavailable ({err}); using the dev hash embedder. \
                 Provision the model under {} (or set GROWLERDB_MODEL_DIR).",
                bge::model_dir(&spec.model).display()
            )),
        }
    }

    #[cfg(not(feature = "bge"))]
    warn_fallback("the `bge` feature is disabled; using the dev hash embedder");

    Arc::new(HashEmbedder::new(spec.model.clone(), spec.dims))
}

/// The EXTERNAL embedder: call a hosted provider over HTTP with a server-side-only key. Fails
/// **closed** — a misconfiguration (no key, no endpoint, or the `external` feature compiled out)
/// surfaces as an [`EmbedError`] at [`Embedder::embed`], never a silent fall back to the dev
/// embedder (which would hide it).
fn external_embedder(spec: &VectorSpec) -> Arc<dyn Embedder> {
    #[cfg(feature = "external")]
    {
        Arc::new(external::ExternalEmbedder::from_env(spec))
    }
    #[cfg(not(feature = "external"))]
    {
        Arc::new(FailClosedEmbedder::new(
            spec,
            "external embedding provider selected but the `external` feature is disabled",
        ))
    }
}

/// A fail-closed [`Embedder`] used when an EXTERNAL field is selected but the `external` feature
/// is compiled out: every [`embed`](Embedder::embed) errors with a clear reason rather than
/// silently producing dev-hash vectors that would hide the misconfiguration.
#[cfg(not(feature = "external"))]
struct FailClosedEmbedder {
    model_id: String,
    dims: usize,
    reason: String,
}

#[cfg(not(feature = "external"))]
impl FailClosedEmbedder {
    fn new(spec: &VectorSpec, reason: &str) -> Self {
        Self {
            model_id: spec.model.clone(),
            dims: spec.dims,
            reason: reason.to_string(),
        }
    }
}

#[cfg(not(feature = "external"))]
impl Embedder for FailClosedEmbedder {
    fn model_id(&self) -> &str {
        &self.model_id
    }
    fn dims(&self) -> usize {
        self.dims
    }
    fn embed(&self, _texts: &[String]) -> Result<Vec<Vec<f32>>, growlerdb_core::EmbedError> {
        Err(growlerdb_core::EmbedError::Backend(self.reason.clone()))
    }
}

/// Log the fallback-to-hash-embedder reason exactly once per process (repeated per-field
/// ingest calls would otherwise spam it).
fn warn_fallback(msg: &str) {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| tracing::warn!("{msg}"));
}

/// Populate every vector field's embedding on `docs` in place, resolving the embedder via
/// [`embedder_for`] (LOCAL BGE-or-fallback, or the EXTERNAL provider). Mirrors
/// `growlerdb_core::embed_vector_fields` but keyed on this crate's factory rather than core's
/// built-in one.
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

/// Shared core: fill in each vector field's embedding across `docs`, batching the embed call per
/// field. Best-effort (a missing source text embeds `""`), but never silently: an EXTERNAL field
/// that fails closed (no key/endpoint) skips the field with a warning rather than writing
/// dev-hash vectors; a LOCAL batch failure retries **per text** so one poison doc can't void the
/// whole batch's vectors, and whatever is still skipped is counted in a per-call warning — a
/// 20k-doc build losing its vectors must be loud, not a log-free `continue`.
fn embed_docs(idx: &ResolvedIndex, docs: &mut [&mut Document]) {
    for f in &idx.fields {
        let Some(spec) = f.vector.as_ref() else {
            continue;
        };
        let embedder = embedder_for(spec);
        let texts: Vec<String> = docs
            .iter()
            .map(|d| source_text(d, &spec.source_field))
            .collect();
        embed_field(embedder.as_ref(), &f.path, spec.provider, docs, &texts);
    }
}

/// Embed one vector field across `docs` (`texts[i]` is `docs[i]`'s source text). Split from
/// [`embed_docs`] so the failure semantics are testable without a real failing model.
fn embed_field(
    embedder: &dyn Embedder,
    path: &str,
    provider: EmbedProvider,
    docs: &mut [&mut Document],
    texts: &[String],
) {
    match embedder.embed(texts) {
        Ok(vectors) => {
            for (d, v) in docs.iter_mut().zip(vectors) {
                d.fields.insert(path.to_string(), Value::Vector(v));
            }
        }
        Err(err) if provider == EmbedProvider::External => {
            // Fail closed + observable: don't write vectors, but surface why.
            warn_external_embed(&format!(
                "external embedding of field `{path}` failed: {err}; field left un-embedded"
            ));
        }
        Err(batch_err) => {
            let mut skipped = 0usize;
            let mut first_doc_err: Option<EmbedError> = None;
            for (d, text) in docs.iter_mut().zip(texts) {
                match embedder.embed(std::slice::from_ref(text)) {
                    Ok(mut v) if !v.is_empty() => {
                        d.fields
                            .insert(path.to_string(), Value::Vector(v.swap_remove(0)));
                    }
                    Ok(_) => skipped += 1,
                    Err(e) => {
                        skipped += 1;
                        first_doc_err.get_or_insert(e);
                    }
                }
            }
            tracing::warn!(
                field = %path,
                batch = docs.len(),
                skipped,
                batch_error = %batch_err,
                first_doc_error = first_doc_err.map(|e| e.to_string()).unwrap_or_default(),
                "batch embed failed; retried per text — {skipped} of {} doc(s) left un-embedded",
                docs.len(),
            );
        }
    }
}

/// Log an external-embedding failure at most once per process (per-field/per-batch ingest calls
/// would otherwise spam it).
fn warn_external_embed(msg: &str) {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| tracing::warn!("{msg}"));
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
        let _g = crate::env_guard();
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
    fn external_provider_fails_closed_without_a_key() {
        // provider: External + no GROWLERDB_EMBEDDING_API_KEY ⇒ embed() errors (fail closed),
        // never a silent hash-embedder fallback that would hide the misconfiguration.
        let _g = crate::env_guard();
        std::env::remove_var("GROWLERDB_EMBEDDING_API_KEY");
        std::env::set_var("GROWLERDB_EMBEDDING_ENDPOINT", "http://127.0.0.1:1/embed");

        let mut s = spec(384);
        s.provider = EmbedProvider::External;
        let e = embedder_for(&s);
        let err = e.embed(&["hello".into()]).unwrap_err();
        assert!(
            matches!(&err, growlerdb_core::EmbedError::Backend(m) if m.contains("GROWLERDB_EMBEDDING_API_KEY")),
            "expected a fail-closed key error, got {err:?}"
        );

        std::env::remove_var("GROWLERDB_EMBEDDING_ENDPOINT");
    }

    #[test]
    fn reranker_for_falls_back_to_hash_when_no_model() {
        // No provisioned model dir → the deterministic token-overlap reranker, for both the
        // `rerank`-on (load fails → fallback) and `rerank`-off (feature gate) builds.
        let _g = crate::env_guard();
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

    /// A batch-call embedder that fails whenever the batch contains the poison text, and
    /// per-text fails only on the poison itself — the "one bad doc" ingest scenario (the arXiv
    /// demo lost all 20k of a chunk's vectors to a single over-long abstract pre-truncation).
    struct PoisonEmbedder;

    impl Embedder for PoisonEmbedder {
        fn model_id(&self) -> &str {
            "poison-test"
        }
        fn dims(&self) -> usize {
            2
        }
        fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
            if texts.iter().any(|t| t == "POISON") {
                return Err(EmbedError::Backend("poison text".into()));
            }
            Ok(texts.iter().map(|_| vec![1.0, 0.0]).collect())
        }
    }

    fn plain_doc(id: i64, body: &str) -> Document {
        let mut fields = std::collections::BTreeMap::new();
        fields.insert("body".to_string(), Value::Str(body.to_string()));
        Document::new(
            growlerdb_core::CompositeKey::new(vec![], vec![("id".into(), Value::Int(id))]),
            fields,
        )
    }

    /// Regression (TASK-323): a batch embed failure used to void the ENTIRE batch's vectors with
    /// a bare `continue` — one poison doc silently left every other doc un-embedded (and thus
    /// invisible to KNN). The per-text fallback must skip only the true failures.
    #[test]
    fn one_poison_text_no_longer_voids_the_batch() {
        let mut docs = [
            plain_doc(1, "fine"),
            plain_doc(2, "POISON"),
            plain_doc(3, "also fine"),
        ];
        let mut refs: Vec<&mut Document> = docs.iter_mut().collect();
        let texts: Vec<String> = refs.iter().map(|d| source_text(d, "body")).collect();
        embed_field(
            &PoisonEmbedder,
            "body_vec",
            EmbedProvider::Local,
            &mut refs,
            &texts,
        );
        assert!(
            matches!(docs[0].fields.get("body_vec"), Some(Value::Vector(_))),
            "healthy doc 1 must keep its vector"
        );
        assert!(
            !docs[1].fields.contains_key("body_vec"),
            "only the poison doc is skipped"
        );
        assert!(
            matches!(docs[2].fields.get("body_vec"), Some(Value::Vector(_))),
            "healthy doc 3 must keep its vector"
        );
    }

    /// An EXTERNAL provider keeps fail-closed semantics: a batch failure skips the whole field
    /// (no per-text retry against a broken/misconfigured remote), never writes partial vectors.
    #[test]
    fn external_batch_failure_stays_fail_closed() {
        let mut docs = [plain_doc(1, "fine"), plain_doc(2, "POISON")];
        let mut refs: Vec<&mut Document> = docs.iter_mut().collect();
        let texts: Vec<String> = refs.iter().map(|d| source_text(d, "body")).collect();
        embed_field(
            &PoisonEmbedder,
            "body_vec",
            EmbedProvider::External,
            &mut refs,
            &texts,
        );
        assert!(!docs[0].fields.contains_key("body_vec"));
        assert!(!docs[1].fields.contains_key("body_vec"));
    }
}
