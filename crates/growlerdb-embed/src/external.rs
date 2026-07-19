//! The opt-in **external** embedding/rerank path: call a hosted provider over HTTP with a
//! server-side-only API key ([`ProviderSecrets`](crate::ProviderSecrets)). Selected per vector
//! field by `provider == External` (embedding) or by `GROWLERDB_RERANK_PROVIDER=external`
//! (reranking). GrowlerDB calls only **embedding** and **reranking** providers — never an LLM
//! ([D42]).
//!
//! # Configuration (all server-side env)
//!
//! | What                | Env var                        | Source                    |
//! |---------------------|--------------------------------|---------------------------|
//! | Embedding endpoint  | `GROWLERDB_EMBEDDING_ENDPOINT` | full URL of the POST route |
//! | Embedding API key   | `GROWLERDB_EMBEDDING_API_KEY`  | `ProviderSecrets`         |
//! | Embedding model     | the vector field's `model`     | `VectorSpec.model`        |
//! | Embedding dims      | the vector field's `dims`      | `VectorSpec.dims`         |
//! | Rerank endpoint     | `GROWLERDB_RERANK_ENDPOINT`    | full URL of the POST route |
//! | Rerank API key      | `GROWLERDB_RERANK_API_KEY`     | `ProviderSecrets`         |
//! | Rerank model        | the reranker `model_id`        | factory arg               |
//!
//! # Fail closed
//!
//! If `External` is selected but the API key is unset, [`embed`](ExternalEmbedder::embed) /
//! [`rerank`](ExternalReranker::rerank) return a clear [`EmbedError::Backend`] — they do **not**
//! silently fall back to the dev embedder/reranker, which would hide a misconfiguration.
//!
//! # Wire format
//!
//! OpenAI/Voyage-style JSON. Embedding request `{"model":…,"input":[texts]}`; response accepted as
//! either `{"data":[{"embedding":[…]}]}` (OpenAI) or `{"embeddings":[[…]]}` (Voyage). Rerank
//! request `{"model":…,"query":…,"documents":[…]}`; response `{"results":[{"index":i,
//! "relevance_score":s}]}` (Cohere/Voyage rerank). Providers that diverge from these shapes need a
//! small per-provider adapter (deferred).
//!
//! [D42]: ../../../okf/system/decisions/d42-retrieval-first.md

use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use growlerdb_core::index_def::VectorSpec;
use growlerdb_core::{EmbedError, Embedder, Reranker};

use crate::secrets::ProviderSecrets;

/// Env var giving the full URL the external embedder POSTs to.
pub const EMBEDDING_ENDPOINT_ENV: &str = "GROWLERDB_EMBEDDING_ENDPOINT";
/// Env var giving the full URL the external reranker POSTs to.
pub const RERANK_ENDPOINT_ENV: &str = "GROWLERDB_RERANK_ENDPOINT";
/// Env var selecting the external reranker (`external` = on; anything else = local).
pub const RERANK_PROVIDER_ENV: &str = "GROWLERDB_RERANK_PROVIDER";

/// Outbound HTTP timeout for a single provider call. Embedding a batch can be a few seconds; keep
/// it bounded so a hung provider can't wedge ingest/search.
const HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// True when the reranker is configured to use the external provider
/// (`GROWLERDB_RERANK_PROVIDER=external`, case-insensitive). Re-read each call.
pub fn rerank_provider_is_external() -> bool {
    std::env::var(RERANK_PROVIDER_ENV)
        .map(|v| v.trim().eq_ignore_ascii_case("external"))
        .unwrap_or(false)
}

/// An [`Embedder`] that calls a hosted embedding provider over HTTP. Holds only config +
/// [`ProviderSecrets`] (a zero-sized env handle); the API key and endpoint are re-read on each
/// [`embed`](Self::embed) so rotation and misconfiguration are handled at call time (fail closed).
pub struct ExternalEmbedder {
    model: String,
    dims: usize,
    secrets: ProviderSecrets,
}

impl ExternalEmbedder {
    /// Build an external embedder for `spec` (model + dims from the field). The endpoint and key
    /// are resolved from the environment lazily, on each [`embed`](Self::embed).
    pub fn from_env(spec: &VectorSpec) -> Self {
        Self {
            model: spec.model.clone(),
            dims: spec.dims,
            secrets: ProviderSecrets::from_env(),
        }
    }
}

impl Embedder for ExternalEmbedder {
    fn model_id(&self) -> &str {
        &self.model
    }

    fn dims(&self) -> usize {
        self.dims
    }

    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        if texts.is_empty() {
            return Ok(vec![]);
        }
        // Fail closed: no key ⇒ a clear error, never a silent fall back to the dev embedder.
        let key = self.secrets.embedding_key().ok_or_else(|| {
            EmbedError::Backend(format!(
                "external embedding provider selected but {} is not set",
                crate::secrets::EMBEDDING_API_KEY_ENV
            ))
        })?;
        let endpoint = endpoint_from_env(EMBEDDING_ENDPOINT_ENV)?;

        let req = EmbedRequest {
            model: &self.model,
            input: texts,
        };
        let resp: EmbedResponse = post_json(&endpoint, &key, &req)?;
        let vectors = resp.into_vectors()?;

        if vectors.len() != texts.len() {
            return Err(EmbedError::Backend(format!(
                "external embedding provider returned {} vectors for {} inputs",
                vectors.len(),
                texts.len()
            )));
        }
        for v in &vectors {
            if v.len() != self.dims {
                return Err(EmbedError::DimMismatch {
                    expected: self.dims,
                    got: v.len(),
                });
            }
        }
        Ok(vectors)
    }
}

/// A [`Reranker`] that calls a hosted rerank provider over HTTP. Analogous to
/// [`ExternalEmbedder`]: config + [`ProviderSecrets`] only; key/endpoint re-read per call.
pub struct ExternalReranker {
    model: String,
    secrets: ProviderSecrets,
}

impl ExternalReranker {
    /// Build an external reranker for `model_id`. Endpoint and key are resolved from the
    /// environment lazily, on each [`rerank`](Self::rerank).
    pub fn from_env(model_id: &str) -> Self {
        Self {
            model: model_id.to_string(),
            secrets: ProviderSecrets::from_env(),
        }
    }
}

impl Reranker for ExternalReranker {
    fn model_id(&self) -> &str {
        &self.model
    }

    fn rerank(
        &self,
        query: &str,
        docs: &[String],
        top_k: usize,
    ) -> Result<Vec<(usize, f32)>, EmbedError> {
        if docs.is_empty() {
            return Ok(vec![]);
        }
        // Fail closed: no key ⇒ a clear error, never a silent fall back to the dev reranker.
        let key = self.secrets.rerank_key().ok_or_else(|| {
            EmbedError::Backend(format!(
                "external rerank provider selected but {} is not set",
                crate::secrets::RERANK_API_KEY_ENV
            ))
        })?;
        let endpoint = endpoint_from_env(RERANK_ENDPOINT_ENV)?;

        let req = RerankRequest {
            model: &self.model,
            query,
            documents: docs,
        };
        let resp: RerankResponse = post_json(&endpoint, &key, &req)?;

        let mut scored: Vec<(usize, f32)> = resp
            .results
            .into_iter()
            .filter(|r| r.index < docs.len())
            .map(|r| (r.index, r.relevance_score))
            .collect();
        // Best-first; stable tiebreak on original index (matches the local reranker's contract).
        scored.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        scored.truncate(top_k);
        Ok(scored)
    }
}

/// The endpoint URL from `env_var`, or a clear [`EmbedError::Backend`] when unset/blank.
fn endpoint_from_env(env_var: &str) -> Result<String, EmbedError> {
    std::env::var(env_var)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            EmbedError::Backend(format!(
                "external provider selected but {env_var} (the endpoint URL) is not set"
            ))
        })
}

/// POST `body` as JSON to `url` with an `Authorization: Bearer <key>` header and parse the JSON
/// response into `T`.
///
/// The call runs on a **dedicated OS thread**: [`embed`](Embedder::embed) is synchronous but is
/// invoked from inside the engine's async (Tokio) runtime, and a `reqwest::blocking` client cannot
/// be built or driven from within a runtime. Handing the work to a plain thread sidesteps that
/// entirely. The raw key is never logged — only transport/status/parse errors surface.
fn post_json<B: Serialize, T: DeserializeOwned + Send>(
    url: &str,
    key: &str,
    body: &B,
) -> Result<T, EmbedError> {
    let payload = serde_json::to_vec(body)
        .map_err(|e| EmbedError::Backend(format!("serialize provider request: {e}")))?;

    std::thread::scope(|scope| {
        scope
            .spawn(|| -> Result<T, EmbedError> {
                let client = reqwest::blocking::Client::builder()
                    .timeout(HTTP_TIMEOUT)
                    .build()
                    .map_err(|e| EmbedError::Backend(format!("build http client: {e}")))?;
                let resp = client
                    .post(url)
                    .bearer_auth(key)
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(payload)
                    .send()
                    .map_err(|e| EmbedError::Backend(format!("provider request failed: {e}")))?;

                let status = resp.status();
                if !status.is_success() {
                    // Body may echo request context but must not include our key; surface status +
                    // a bounded snippet for diagnosis.
                    let snippet = resp.text().unwrap_or_default();
                    let snippet: String = snippet.chars().take(200).collect();
                    return Err(EmbedError::Backend(format!(
                        "provider returned HTTP {status}: {snippet}"
                    )));
                }
                resp.json::<T>()
                    .map_err(|e| EmbedError::Backend(format!("parse provider response: {e}")))
            })
            .join()
            .map_err(|_| EmbedError::Backend("provider request thread panicked".to_string()))?
    })
}

#[derive(Serialize)]
struct EmbedRequest<'a> {
    model: &'a str,
    input: &'a [String],
}

/// Accept both the OpenAI (`data[].embedding`) and Voyage (`embeddings[]`) response shapes.
#[derive(Deserialize)]
struct EmbedResponse {
    #[serde(default)]
    data: Vec<EmbedDatum>,
    #[serde(default)]
    embeddings: Vec<Vec<f32>>,
}

#[derive(Deserialize)]
struct EmbedDatum {
    embedding: Vec<f32>,
}

impl EmbedResponse {
    fn into_vectors(self) -> Result<Vec<Vec<f32>>, EmbedError> {
        if !self.data.is_empty() {
            Ok(self.data.into_iter().map(|d| d.embedding).collect())
        } else if !self.embeddings.is_empty() {
            Ok(self.embeddings)
        } else {
            Err(EmbedError::Backend(
                "external embedding response had neither `data[].embedding` nor `embeddings[]`"
                    .to_string(),
            ))
        }
    }
}

#[derive(Serialize)]
struct RerankRequest<'a> {
    model: &'a str,
    query: &'a str,
    documents: &'a [String],
}

#[derive(Deserialize)]
struct RerankResponse {
    #[serde(default)]
    results: Vec<RerankResult>,
}

#[derive(Deserialize)]
struct RerankResult {
    index: usize,
    relevance_score: f32,
}

#[cfg(test)]
mod tests {
    use super::*;
    // Crate-wide env lock so these tests serialize against the env-mutating tests in
    // `secrets.rs` / `lib.rs` too (they share the same process-global vars).
    use crate::env_guard;

    fn spec() -> VectorSpec {
        use growlerdb_core::index_def::{EmbedProvider, VectorMetric};
        VectorSpec {
            dims: 3,
            model: "text-embed-test".into(),
            metric: VectorMetric::Cosine,
            provider: EmbedProvider::External,
            source_field: "body".into(),
        }
    }

    #[test]
    fn embed_fails_closed_without_a_key() {
        let _g = env_guard();
        std::env::remove_var(crate::secrets::EMBEDDING_API_KEY_ENV);
        std::env::set_var(EMBEDDING_ENDPOINT_ENV, "http://127.0.0.1:1/embed");

        let e = ExternalEmbedder::from_env(&spec());
        let err = e.embed(&["hello".into()]).unwrap_err();
        match err {
            EmbedError::Backend(msg) => {
                assert!(msg.contains("GROWLERDB_EMBEDDING_API_KEY"), "{msg}");
                assert!(msg.contains("not set"), "{msg}");
            }
            other => panic!("expected a fail-closed Backend error, got {other:?}"),
        }
        std::env::remove_var(EMBEDDING_ENDPOINT_ENV);
    }

    #[test]
    fn rerank_fails_closed_without_a_key() {
        let _g = env_guard();
        std::env::remove_var(crate::secrets::RERANK_API_KEY_ENV);
        std::env::set_var(RERANK_ENDPOINT_ENV, "http://127.0.0.1:1/rerank");

        let r = ExternalReranker::from_env("rerank-test");
        let err = r.rerank("q", &["a".into(), "b".into()], 2).unwrap_err();
        match err {
            EmbedError::Backend(msg) => assert!(msg.contains("GROWLERDB_RERANK_API_KEY"), "{msg}"),
            other => panic!("expected a fail-closed Backend error, got {other:?}"),
        }
        std::env::remove_var(RERANK_ENDPOINT_ENV);
    }

    #[test]
    fn empty_input_short_circuits_without_a_call() {
        // No endpoint/key set at all, but empty input must not attempt a request.
        assert!(ExternalEmbedder::from_env(&spec())
            .embed(&[])
            .unwrap()
            .is_empty());
        assert!(ExternalReranker::from_env("m")
            .rerank("q", &[], 5)
            .unwrap()
            .is_empty());
    }

    /// Drive the real HTTP path against a tiny in-process axum mock (offline, `127.0.0.1:0`):
    /// assert the `Authorization: Bearer <key>` header is sent and an OpenAI-shaped response
    /// parses into vectors. Multi-thread runtime + `spawn_blocking` so the synchronous, thread
    /// -bridged `embed()` runs while the mock server makes progress — mirroring the real
    /// call-from-async-runtime scenario.
    // The env guard intentionally serializes this test against the other env-mutating tests; it is
    // held across `.await` on purpose (the whole test is a critical section over process env). No
    // other task contends for this std Mutex, so it can't deadlock the runtime.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn embed_calls_mock_provider_with_bearer_auth_and_parses() {
        use std::sync::{Arc, Mutex};

        use axum::extract::State;
        use axum::http::HeaderMap;
        use axum::routing::post;
        use axum::{Json, Router};

        let _g = env_guard();

        // Captures what the mock provider saw, so the test can assert on the header/body.
        #[derive(Default)]
        struct Seen {
            authorization: Option<String>,
            model: Option<String>,
        }
        let seen = Arc::new(Mutex::new(Seen::default()));

        async fn handler(
            State(seen): State<Arc<Mutex<Seen>>>,
            headers: HeaderMap,
            Json(body): Json<serde_json::Value>,
        ) -> Json<serde_json::Value> {
            let mut s = seen.lock().unwrap();
            s.authorization = headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            s.model = body
                .get("model")
                .and_then(|m| m.as_str())
                .map(str::to_string);
            Json(serde_json::json!({ "data": [{ "embedding": [0.1, 0.2, 0.3] }] }))
        }

        let app = Router::new()
            .route("/embed", post(handler))
            .with_state(seen.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        std::env::set_var(EMBEDDING_ENDPOINT_ENV, format!("http://{addr}/embed"));
        std::env::set_var(crate::secrets::EMBEDDING_API_KEY_ENV, "sk-mock-embed-key");

        let embedder = ExternalEmbedder::from_env(&spec());
        let out = tokio::task::spawn_blocking(move || embedder.embed(&["hello world".into()]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out, vec![vec![0.1, 0.2, 0.3]]);

        let s = seen.lock().unwrap();
        assert_eq!(
            s.authorization.as_deref(),
            Some("Bearer sk-mock-embed-key"),
            "the server-side key must be sent as a Bearer token"
        );
        assert_eq!(s.model.as_deref(), Some("text-embed-test"));

        std::env::remove_var(EMBEDDING_ENDPOINT_ENV);
        std::env::remove_var(crate::secrets::EMBEDDING_API_KEY_ENV);
    }

    #[test]
    fn embed_response_parses_openai_and_voyage_shapes() {
        let openai: EmbedResponse =
            serde_json::from_str(r#"{"data":[{"embedding":[0.1,0.2,0.3]}]}"#).unwrap();
        assert_eq!(openai.into_vectors().unwrap(), vec![vec![0.1, 0.2, 0.3]]);

        let voyage: EmbedResponse =
            serde_json::from_str(r#"{"embeddings":[[1.0,2.0],[3.0,4.0]]}"#).unwrap();
        assert_eq!(
            voyage.into_vectors().unwrap(),
            vec![vec![1.0, 2.0], vec![3.0, 4.0]]
        );

        let empty: EmbedResponse = serde_json::from_str(r#"{"data":[]}"#).unwrap();
        assert!(empty.into_vectors().is_err());
    }
}
