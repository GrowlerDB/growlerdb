//! The Node **Suggest** gRPC service ([Design 01]) — term suggestions over an
//! indexed field's dictionary (task-25): **autocomplete** (prefix completions) and
//! **did-you-mean** (fuzzy corrections). Adapts the in-process `Shard::suggest_prefix`
//! / `suggest_fuzzy`. Suggestion frequencies are approximate (not liveness-filtered),
//! the suggester contract.
//!
//! [Design 01]: ../../../design/01-engine-api.md

use growlerdb_index::{IndexError, StoreError};
use growlerdb_proto::v1::{
    Error as WireError, SuggestKind, SuggestRequest, SuggestResponse, Suggestion,
};
use growlerdb_proto::{to_status, Suggest, SuggestServer};
use tonic::{Code, Request, Response, Status};

use crate::auth::{self, default_auth, SharedAuth};
use crate::service_util::internal;
use crate::shard_handle::ShardHandle;

/// Hits-per-suggestion default when `limit` is 0.
const DEFAULT_LIMIT: usize = 10;
/// Default and ceiling for the fuzzy edit distance (keeps the dictionary scan cheap).
const DEFAULT_MAX_EDITS: u8 = 2;
const MAX_MAX_EDITS: u8 = 3;

/// A `Suggest` service over one shard. Term-dictionary scans are blocking, so they run
/// on the blocking pool. Every RPC consults the [auth hook](SharedAuth) first.
#[derive(Clone)]
pub struct SuggestService {
    shard: ShardHandle,
    auth: SharedAuth,
}

impl SuggestService {
    /// A Suggest service over `shard` with the default no-op auth hook. Accepts an
    /// `Arc<Shard>` (fresh handle) or a shared [`ShardHandle`].
    pub fn new(shard: impl Into<ShardHandle>) -> Self {
        Self::with_auth(shard, default_auth())
    }

    /// As [`new`](Self::new), with a specific [auth hook](SharedAuth).
    pub fn with_auth(shard: impl Into<ShardHandle>, auth: SharedAuth) -> Self {
        Self {
            shard: shard.into(),
            auth,
        }
    }

    /// Wrap as a mountable tonic [`SuggestServer`].
    pub fn into_server(self) -> SuggestServer<Self> {
        SuggestServer::new(self)
    }
}

#[tonic::async_trait]
impl Suggest for SuggestService {
    async fn suggest(
        &self,
        request: Request<SuggestRequest>,
    ) -> Result<Response<SuggestResponse>, Status> {
        auth::authorize(&self.auth, "Suggest", &request)?;
        let req = request.into_inner();
        let invalid = |msg: &str| {
            to_status(
                Code::InvalidArgument,
                WireError::new("INVALID_ARGUMENT", msg.to_string()),
            )
        };

        if req.field.is_empty() {
            return Err(invalid("suggest requires a `field`"));
        }
        if req.text.is_empty() {
            return Err(invalid("suggest requires non-empty `text`"));
        }
        let kind =
            SuggestKind::try_from(req.kind).map_err(|_| invalid("unknown suggest `kind`"))?;
        let limit = if req.limit == 0 {
            DEFAULT_LIMIT
        } else {
            req.limit as usize
        };
        // Node-level ceiling (task-146 / F13): a Node is directly reachable in distributed mode, so
        // cap an unbounded `limit` before it drives a huge scan.
        if limit > crate::search_service::MAX_NODE_FETCH {
            return Err(invalid("limit exceeds the maximum page fetch (10000)"));
        }
        let max_edits = match req.max_edits {
            0 => DEFAULT_MAX_EDITS,
            n => (n as u8).min(MAX_MAX_EDITS),
        };

        let shard = self.shard.current();
        // Tenant scoping (task-38): suggest scans a field's term dictionary across all docs,
        // which a per-doc filter can't constrain — so it would leak other tenants' terms.
        // Fail closed on a tenant-scoped index until a tenant-aware suggester lands.
        if let Some(field) = shard.tenant_field() {
            return Err(to_status(
                Code::PermissionDenied,
                WireError::new(
                    "PERMISSION_DENIED",
                    format!(
                        "suggest is not available on a tenant-scoped index (tenant_field `{field}`): \
                         term suggestions are not yet tenant-filtered"
                    ),
                ),
            ));
        }
        let terms = tokio::task::spawn_blocking(move || match kind {
            SuggestKind::Prefix => shard.suggest_prefix(&req.field, &req.text, limit),
            SuggestKind::Fuzzy => shard.suggest_fuzzy(&req.field, &req.text, max_edits, limit),
        })
        .await
        .map_err(internal)?
        .map_err(store_status)?;

        Ok(Response::new(SuggestResponse {
            suggestions: terms
                .into_iter()
                .map(|(text, count)| Suggestion { text, count })
                .collect(),
            failed_shards: 0, // a Node serves one shard; the Gateway sets this on merge
        }))
    }
}

/// Map a store error to a gRPC status. A bad field / query-shape error (an
/// [`IndexError`] of the request-validation kind) is a client-facing
/// `InvalidArgument`; anything else is `Internal`.
fn store_status(e: StoreError) -> Status {
    match e {
        StoreError::Segment(
            ref inner @ (IndexError::UnknownField(_)
            | IndexError::QueryType(_)
            | IndexError::NoDefaultField
            | IndexError::CostGuard(_)
            | IndexError::Query(_)),
        ) => to_status(
            Code::InvalidArgument,
            WireError::new("INVALID_ARGUMENT", inner.to_string()),
        ),
        other => to_status(
            Code::Internal,
            WireError::new("INTERNAL", other.to_string()),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use growlerdb_core::{
        CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc,
        SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
    };
    use growlerdb_index::{LocalIndexStore, ShardId};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn service(root: &std::path::Path) -> SuggestService {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("city", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: city, type: KEYWORD } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let shard = LocalIndexStore::open(root)
            .unwrap()
            .create_shard(&ShardId::single("docs"), &idx)
            .unwrap();
        let put = |id: &str, city: &str| {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
            let mut f = BTreeMap::new();
            f.insert("id".to_string(), Value::from(id));
            f.insert("city".to_string(), Value::from(city));
            (
                key.clone(),
                LocatedDoc {
                    doc: Document::new(key, f),
                    iceberg_file: "f".into(),
                    row_position: 0,
                },
            )
        };
        let docs = vec![
            put("1", "berlin").1,
            put("2", "berlin").1,
            put("3", "bern").1,
            put("4", "boston").1,
        ];
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(docs, SourceCheckpoint::iceberg(1), "b1"),
        )
        .unwrap();
        SuggestService::new(Arc::new(shard))
    }

    fn texts(resp: &SuggestResponse) -> Vec<(String, u64)> {
        resp.suggestions
            .iter()
            .map(|s| (s.text.clone(), s.count))
            .collect()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prefix_autocomplete_over_the_wire() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        let resp = svc
            .suggest(Request::new(SuggestRequest {
                field: "city".into(),
                text: "ber".into(),
                limit: 10,
                kind: SuggestKind::Prefix as i32,
                max_edits: 0,
                window: 0,
                index: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            texts(&resp),
            vec![("berlin".to_string(), 2), ("bern".to_string(), 1)]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fuzzy_did_you_mean_over_the_wire() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        // "bostom" → boston (distance 1); excludes the input; berlin/bern are farther.
        let resp = svc
            .suggest(Request::new(SuggestRequest {
                field: "city".into(),
                text: "bostom".into(),
                limit: 10,
                kind: SuggestKind::Fuzzy as i32,
                max_edits: 1,
                window: 0,
                index: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(texts(&resp), vec![("boston".to_string(), 1)]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bad_requests_are_invalid_argument() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        let req = |field: &str, text: &str| SuggestRequest {
            field: field.into(),
            text: text.into(),
            limit: 0,
            kind: SuggestKind::Prefix as i32,
            max_edits: 0,
            window: 0,
            index: String::new(),
        };
        // Empty field / empty text are rejected up front.
        for r in [req("", "ber"), req("city", "")] {
            let err = svc.suggest(Request::new(r)).await.unwrap_err();
            assert_eq!(err.code(), Code::InvalidArgument);
        }
        // An unknown field surfaces as InvalidArgument, not Internal.
        let err = svc
            .suggest(Request::new(req("nope", "ber")))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }
}
