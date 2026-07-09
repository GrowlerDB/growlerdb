//! The Node **Search** gRPC service ([Design 01]) — the read side of the Engine
//! API. Adapts the in-process [`IndexReader`] (query execution over the local index,
//! task-21/22/23) to the wire `Search` service, returning ranked document
//! **coordinates** + scores. Hydration of the authoritative rows is a follow-up.
//!
//! [Design 01]: ../../../design/01-engine-api.md

use growlerdb_core::{Agg, CompositeKey, Query, SearchAfter, Sort, SortOrder};
use growlerdb_index::{IndexError, Shard, StoreError};
use growlerdb_proto::v1::{
    AggregateRequest, AggregateResponse, AnalyzedField, ClosePitRequest, ClosePitResponse,
    Error as WireError, ExplainClause, ExplainRequest, ExplainResponse, ExplainTimings,
    ExportRequest, Field, OpenPitRequest, OpenPitResponse, QuerySyntax, SearchHit, SearchRequest,
    SearchResponse,
};
use growlerdb_proto::{to_status, Search, SearchServer};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Request, Response, Status};

use crate::auth::{self, default_auth, SharedAuth};
use crate::service_util::internal;
use crate::shard_handle::ShardHandle;

/// Default hits-per-page for [`Export`](SearchService::export) when the request
/// leaves `page_size` at 0.
const DEFAULT_EXPORT_PAGE_SIZE: usize = 1000;

/// Hard `offset+limit` (and suggest `limit`) ceiling enforced by the Node services (task-146 / F13).
/// The Gateway caps page fetches, but a Node is directly reachable in distributed mode, so it must
/// self-defend against an unbounded `limit` that would build a giant top-k and OOM the process.
/// Matches the Gateway's default `max_fetch`.
pub(crate) const MAX_NODE_FETCH: usize = 10_000;

/// A `Search` service over one shard. Query execution is blocking (Tantivy + redb),
/// so it runs on the blocking pool. Every RPC consults the [auth hook](SharedAuth)
/// (no-op by default; task-19 seam) before serving.
#[derive(Clone)]
pub struct SearchService {
    shard: ShardHandle,
    auth: SharedAuth,
}

impl SearchService {
    /// A Search service over `shard` with the default no-op auth hook. Accepts an
    /// `Arc<Shard>` (fresh handle) or a shared [`ShardHandle`] (so a reindex swap is
    /// visible across services).
    pub fn new(shard: impl Into<ShardHandle>) -> Self {
        Self::with_auth(shard, default_auth())
    }

    /// A Search service over `shard` with a specific [auth hook](SharedAuth).
    pub fn with_auth(shard: impl Into<ShardHandle>, auth: SharedAuth) -> Self {
        Self {
            shard: shard.into(),
            auth,
        }
    }

    /// Wrap as a mountable tonic [`SearchServer`].
    pub fn into_server(self) -> SearchServer<Self> {
        SearchServer::new(self)
    }
}

#[tonic::async_trait]
impl Search for SearchService {
    #[tracing::instrument(name = "node.search", skip_all, err)]
    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        auth::authorize(&self.auth, "Search", &request)?;
        // Cold-window pre-warm signal (task-153 / I3): count real searches, so a promoted-when-hot
        // decision reflects query load, not incidental `current()` calls from other services.
        self.shard.record_search();
        let tenant = auth::tenant_of(&request);
        let req = request.into_inner();
        let invalid =
            |e: String| to_status(Code::InvalidArgument, WireError::new("INVALID_ARGUMENT", e));

        // Parse the query string with the requested grammar — Lucene (default) or KQL (task-90).
        let query = if req.syntax == QuerySyntax::Kql as i32 {
            Query::parse_kql(&req.query)
        } else {
            Query::parse(&req.query)
        }
        .map_err(|e| invalid(e.to_string()))?;
        let k = req.limit as usize;
        let mut sort = Vec::with_capacity(req.sort.len());
        for s in req.sort {
            let order = if s.descending {
                SortOrder::Desc
            } else {
                SortOrder::Asc
            };
            sort.push(Sort {
                field: s.field,
                order,
            });
        }
        // Decode the opaque keyset cursor (an encoded `SearchAfter`), if present.
        let after = if req.search_after.is_empty() {
            None
        } else {
            Some(decode_cursor(&req.search_after).map_err(invalid)?)
        };
        let offset = req.offset as usize;
        // Page-fetch ceiling at the Node too (task-146 / F13): the Gateway caps `offset+limit`, but
        // the Node is a real endpoint (RemoteNode connects directly to it in distributed mode), so a
        // direct RPC with a giant `limit` would build an enormous top-k and OOM the Node, bypassing
        // the Gateway guard. Mirror the Gateway's default ceiling here.
        if offset.saturating_add(k) > MAX_NODE_FETCH {
            return Err(invalid(format!(
                "offset+limit ({}) exceeds the maximum page fetch ({MAX_NODE_FETCH})",
                offset.saturating_add(k)
            )));
        }
        let collapse = req.collapse;
        let pit_id = req.pit_id;

        // Collapse needs a sort to define each group's "top"; reject early & clearly.
        if !collapse.is_empty() && sort.is_empty() {
            return Err(invalid("collapse requires a non-empty sort".into()));
        }

        // Query execution is blocking; keep it off the async runtime.
        let shard = self.shard.current();
        // Tenant scoping (task-38): if the index is tenant-scoped, AND a mandatory tenant
        // filter from the verified claim — the caller can neither read nor widen past it.
        let query = tenant_scope(&shard, query, tenant.as_deref())?;
        let resp = tokio::task::spawn_blocking(move || {
            if !collapse.is_empty() {
                // Collapse honors a pit_id (frozen snapshot) just like the paged path.
                let groups = if pit_id == 0 {
                    shard.search_collapsed(&query, k, &sort, &collapse)?
                } else {
                    shard.search_collapsed_pit(pit_id, &query, k, &sort, &collapse)?
                };
                return Ok(SearchResponse {
                    total: groups.len() as u64,
                    hits: groups
                        .into_iter()
                        .map(|g| SearchHit {
                            coordinates: Some((&g.hit.key).into()),
                            score: g.hit.score as f64,
                            group: Some(g.group.into()),
                            group_count: g.count as u64,
                            // The group's top-hit sort values, so the Gateway can fold and
                            // order collapse groups across shards (task-68).
                            sort_values: g.sort_values.iter().map(Into::into).collect(),
                            fields: hit_fields(&g.hit.fields),
                        })
                        .collect(),
                    next_cursor: Vec::new(),
                    partial: false,
                    // A bare Node has no shard scope; the Gateway stamps scanned/total (task-133).
                    ..Default::default()
                });
            }
            // A pit_id reads against that frozen snapshot; 0 reads the latest.
            let (hits, next) = if pit_id == 0 {
                shard.search_page_values(&query, k, &sort, offset, after.as_ref())?
            } else {
                shard.search_page_pit_values(pit_id, &query, k, &sort, offset, after.as_ref())?
            };
            // `total` is the true match count (read against the same snapshot), not the page
            // size, so a Gateway can sum it across shards for a global total (task-68).
            let total = if pit_id == 0 {
                shard.search_count(&query)?
            } else {
                shard.search_count_pit(pit_id, &query)?
            };
            Ok(page_response(hits, next, total))
        })
        .await
        .map_err(internal)?
        .map_err(store_status)?;

        Ok(Response::new(resp))
    }

    /// Explain how `query` scores one document (task-102). Locates the doc by coordinate, asks the
    /// index for Tantivy's BM25 explanation tree + analyzed terms, and measures the index-side time.
    /// The Gateway adds hydration/total timings + shard counts. Honors tenant scoping like search.
    #[tracing::instrument(name = "node.explain", skip_all, err)]
    async fn explain(
        &self,
        request: Request<ExplainRequest>,
    ) -> Result<Response<ExplainResponse>, Status> {
        auth::authorize(&self.auth, "Search", &request)?;
        let tenant = auth::tenant_of(&request);
        let req = request.into_inner();
        let invalid =
            |e: String| to_status(Code::InvalidArgument, WireError::new("INVALID_ARGUMENT", e));

        let query = if req.syntax == QuerySyntax::Kql as i32 {
            Query::parse_kql(&req.query)
        } else {
            Query::parse(&req.query)
        }
        .map_err(|e| invalid(e.to_string()))?;
        let coords = req
            .coordinates
            .ok_or_else(|| invalid("explain requires a document coordinate".into()))?;
        let key = CompositeKey::try_from(coords)
            .map_err(|e| invalid(format!("malformed coordinate: {e}")))?;

        let shard = self.shard.current();
        let query = tenant_scope(&shard, query, tenant.as_deref())?;
        let started = std::time::Instant::now();
        let hit = tokio::task::spawn_blocking(move || shard.explain(&query, &key))
            .await
            .map_err(internal)?
            .map_err(store_status)?;
        let index_ms = started.elapsed().as_secs_f64() * 1000.0;

        let analyzed = hit
            .analyzed
            .into_iter()
            .map(|(field, terms)| AnalyzedField { field, terms })
            .collect();
        let detail = if hit.matched {
            json_to_clause(&hit.detail)
        } else {
            None
        };
        Ok(Response::new(ExplainResponse {
            found: hit.found,
            matched: hit.matched,
            score: hit.score as f64,
            detail,
            analyzed,
            // The Node times the index side; the Gateway fills hydration/total + the real shard total.
            timings: Some(ExplainTimings {
                index_ms,
                hydration_ms: 0.0,
                total_ms: index_ms,
            }),
            shards_scanned: u32::from(hit.found),
            shards_total: 1,
        }))
    }

    async fn open_pit(
        &self,
        request: Request<OpenPitRequest>,
    ) -> Result<Response<OpenPitResponse>, Status> {
        auth::authorize(&self.auth, "OpenPit", &request)?;
        let shard = self.shard.current();
        let pit = tokio::task::spawn_blocking(move || shard.open_pit())
            .await
            .map_err(internal)?
            .map_err(store_status)?;
        Ok(Response::new(OpenPitResponse {
            pit_id: pit.id,
            snapshot: pit.snapshot,
        }))
    }

    async fn close_pit(
        &self,
        request: Request<ClosePitRequest>,
    ) -> Result<Response<ClosePitResponse>, Status> {
        auth::authorize(&self.auth, "ClosePit", &request)?;
        let id = request.into_inner().pit_id;
        let shard = self.shard.current();
        let closed = tokio::task::spawn_blocking(move || shard.close_pit(id))
            .await
            .map_err(internal)?;
        Ok(Response::new(ClosePitResponse { closed }))
    }

    type ExportStream = ReceiverStream<Result<SearchResponse, Status>>;

    async fn export(
        &self,
        request: Request<ExportRequest>,
    ) -> Result<Response<Self::ExportStream>, Status> {
        auth::authorize(&self.auth, "Export", &request)?;
        let tenant = auth::tenant_of(&request);
        let req = request.into_inner();
        let invalid = |e: &str| {
            to_status(
                Code::InvalidArgument,
                WireError::new("INVALID_ARGUMENT", e.to_string()),
            )
        };

        let query = Query::parse(&req.query).map_err(|e| {
            to_status(
                Code::InvalidArgument,
                WireError::new("INVALID_ARGUMENT", e.to_string()),
            )
        })?;
        if req.sort.is_empty() {
            return Err(invalid("export requires a non-empty sort"));
        }
        let sort: Vec<Sort> = req
            .sort
            .into_iter()
            .map(|s| Sort {
                field: s.field,
                order: if s.descending {
                    SortOrder::Desc
                } else {
                    SortOrder::Asc
                },
            })
            .collect();
        let page_size = if req.page_size == 0 {
            DEFAULT_EXPORT_PAGE_SIZE
        } else {
            req.page_size as usize
        };
        let given_pit = req.pit_id;

        // Stream pages from a blocking task over a bounded channel (backpressure: the
        // producer parks on `blocking_send` until the client drains).
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<SearchResponse, Status>>(4);
        let shard = self.shard.current();
        // Tenant scoping (task-38): export is a full scan, so the tenant filter matters most here.
        let query = tenant_scope(&shard, query, tenant.as_deref())?;
        tokio::task::spawn_blocking(move || {
            // Use the caller's PIT, or open one for the export's duration (then close).
            let (pit, owned) = if given_pit != 0 {
                (given_pit, false)
            } else {
                match shard.open_pit() {
                    Ok(p) => (p.id, true),
                    Err(e) => {
                        let _ = tx.blocking_send(Err(store_status(e)));
                        return;
                    }
                }
            };

            // The full match count for the export (one count against the frozen PIT), stamped
            // on every streamed page as its `total` — the whole result size, not the chunk size.
            let total = match shard.search_count_pit(pit, &query) {
                Ok(n) => n,
                Err(e) => {
                    let _ = tx.blocking_send(Err(store_status(e)));
                    return;
                }
            };
            let mut cursor: Option<SearchAfter> = None;
            loop {
                match shard.search_page_pit_values(
                    pit,
                    &query,
                    page_size,
                    &sort,
                    0,
                    cursor.as_ref(),
                ) {
                    Ok((hits, next)) => {
                        if hits.is_empty() {
                            break; // exhausted
                        }
                        let resp = page_response(hits, next.clone(), total);
                        if tx.blocking_send(Ok(resp)).is_err() {
                            break; // client hung up
                        }
                        match next {
                            Some(c) => cursor = Some(c),
                            None => break,
                        }
                    }
                    Err(e) => {
                        let _ = tx.blocking_send(Err(store_status(e)));
                        break;
                    }
                }
            }
            if owned {
                shard.close_pit(pit);
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn aggregate(
        &self,
        request: Request<AggregateRequest>,
    ) -> Result<Response<AggregateResponse>, Status> {
        auth::authorize(&self.auth, "Aggregate", &request)?;
        let tenant = auth::tenant_of(&request);
        let req = request.into_inner();
        let invalid =
            |e: String| to_status(Code::InvalidArgument, WireError::new("INVALID_ARGUMENT", e));

        let query = Query::parse(&req.query).map_err(|e| invalid(e.to_string()))?;
        // The spec is a JSON object name → externally-tagged `Agg` (empty ⇒ no aggs).
        let aggs: std::collections::BTreeMap<String, Agg> = if req.aggs.trim().is_empty() {
            std::collections::BTreeMap::new()
        } else {
            serde_json::from_str(&req.aggs).map_err(|e| invalid(format!("aggs: {e}")))?
        };
        let aggs: Vec<(String, Agg)> = aggs.into_iter().collect();
        // Validate the spec (e.g. range buckets ascending/non-overlapping) before it reaches
        // Tantivy, so bad input is a clear InvalidArgument, not an opaque Internal (task-75).
        growlerdb_core::validate_aggs(&aggs).map_err(invalid)?;

        let shard = self.shard.current();
        // Tenant scoping (task-38): aggregations over another tenant's rows would leak counts/
        // sums, so the same mandatory filter applies before the agg runs.
        let query = tenant_scope(&shard, query, tenant.as_deref())?;
        if req.partial {
            // Return the mergeable intermediate form (the Gateway merges across shards).
            let partial =
                tokio::task::spawn_blocking(move || shard.aggregate_partial(&query, &aggs))
                    .await
                    .map_err(|e| {
                        to_status(Code::Internal, WireError::new("INTERNAL", e.to_string()))
                    })?
                    .map_err(store_status)?;
            return Ok(Response::new(AggregateResponse {
                results: String::new(),
                partial,
                failed_shards: 0, // a Node serves one shard; the Gateway sets this on merge
            }));
        }
        let results = tokio::task::spawn_blocking(move || shard.aggregate(&query, &aggs))
            .await
            .map_err(internal)?
            .map_err(store_status)?;
        let results = serde_json::to_string(&results).map_err(internal)?;
        Ok(Response::new(AggregateResponse {
            results,
            partial: Vec::new(),
            failed_shards: 0, // a Node serves one shard; the Gateway sets this on merge
        }))
    }
}

/// Apply **tenant scoping** (task-38) to a read's query. If `shard` is tenant-scoped, the
/// request must carry a verified `tenant` claim, and a mandatory `tenant_field = tenant`
/// filter is AND-ed in (the caller can neither read nor widen past it). A tenant-scoped index
/// with no claim is rejected (`PermissionDenied`) — fail closed. Unscoped indexes pass through.
fn tenant_scope(shard: &Shard, query: Query, tenant: Option<&str>) -> Result<Query, Status> {
    let Some(field) = shard.tenant_field() else {
        return Ok(query);
    };
    let tenant = tenant.ok_or_else(|| {
        to_status(
            Code::PermissionDenied,
            WireError::new(
                "PERMISSION_DENIED",
                format!("index is tenant-scoped on `{field}`; request carries no verified tenant"),
            ),
        )
    })?;
    Ok(query.and_filter(field, tenant))
}

/// Convert Tantivy's serialized `Explanation` JSON (`{value, description, details}`) into the
/// wire [`ExplainClause`] tree (task-102). Returns `None` for a non-object (e.g. null = unmatched).
fn json_to_clause(v: &serde_json::Value) -> Option<ExplainClause> {
    let obj = v.as_object()?;
    Some(ExplainClause {
        description: obj
            .get("description")
            .and_then(|d| d.as_str())
            .unwrap_or_default()
            .to_string(),
        score: obj.get("value").and_then(|s| s.as_f64()).unwrap_or(0.0),
        details: obj
            .get("details")
            .and_then(|d| d.as_array())
            .map(|arr| arr.iter().filter_map(json_to_clause).collect())
            .unwrap_or_default(),
    })
}

/// Build a non-collapsed page response: hits (coordinates + score) + the next-page
/// keyset cursor.
fn page_response(
    hits: Vec<(growlerdb_core::Hit, Vec<growlerdb_core::SortValue>)>,
    next: Option<SearchAfter>,
    total: u64,
) -> SearchResponse {
    SearchResponse {
        total,
        hits: hits
            .iter()
            .map(|(h, sort_values)| SearchHit {
                coordinates: Some((&h.key).into()),
                score: h.score as f64,
                group: None,
                group_count: 0,
                // Field-sorted hits carry their sort values so the Gateway can merge
                // across shards (design/09); score-ranked hits leave this empty.
                sort_values: sort_values.iter().map(Into::into).collect(),
                // Cached display fields (D23/task-86) render the page without hydration.
                fields: hit_fields(&h.fields),
            })
            .collect(),
        next_cursor: next.map(encode_cursor).unwrap_or_default(),
        partial: false,
        // A bare Node has no shard scope; the Gateway stamps scanned/total (task-133).
        ..Default::default()
    }
}

/// The cached display fields (D23) of a core [`Hit`](growlerdb_core::Hit) as wire [`Field`]s — the
/// `cached` fields the index returns with each hit so a page renders document-like rows without a
/// hydration round-trip (task-86). Empty when the index caches no display fields.
fn hit_fields(fields: &std::collections::BTreeMap<String, growlerdb_core::Value>) -> Vec<Field> {
    fields
        .iter()
        .map(|(name, value)| Field {
            name: name.clone(),
            value: Some(value.clone().into()),
        })
        .collect()
}

/// Map a store error to a gRPC status: an unknown/expired PIT is a client-facing
/// `FailedPrecondition` (re-open one); everything else is `Internal`.
fn store_status(e: StoreError) -> Status {
    match e {
        StoreError::UnknownPit(_) => to_status(
            Code::FailedPrecondition,
            WireError::new("PIT_EXPIRED", e.to_string()),
        ),
        // Query-validation failures are the client's fault — an unknown/non-searchable field, a bad
        // query shape, no default field, or a cost-guarded pattern — so they're `InvalidArgument`
        // (HTTP 400), not `Internal` (500). Mirrors the suggest service. (A single-shard Gateway
        // forwards this code verbatim; see `gateway::search_inner`.)
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

/// Encode a keyset cursor to the opaque wire token (JSON bytes — clients treat it as
/// opaque and round-trip it verbatim). Shared with the [Gateway](crate::gateway), which
/// composes the **global** cursor for a multi-shard scroll in this same format so every
/// Node can decode it (task-68).
pub(crate) fn encode_cursor(cursor: SearchAfter) -> Vec<u8> {
    // SearchAfter is plain data (numbers + a composite key); serialization is infallible.
    serde_json::to_vec(&cursor).unwrap_or_default()
}

/// Decode an opaque wire token back into a keyset cursor; a malformed token is a
/// client error.
pub(crate) fn decode_cursor(token: &[u8]) -> std::result::Result<SearchAfter, String> {
    serde_json::from_slice(token).map_err(|_| "invalid search_after cursor".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use growlerdb_core::{
        CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc,
        SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
    };
    use growlerdb_index::{LocalIndexStore, Shard, ShardId};
    use growlerdb_proto::v1::{SearchRequest, Sort as WireSort};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    /// A shard with a `rank` LONG fast field, three docs in two generations.
    fn ranked_service(root: &std::path::Path) -> SearchService {
        SearchService::new(std::sync::Arc::new(ranked_shard(root)))
    }

    /// The bare [`Shard`] behind [`ranked_service`] — for tests that need to swap the handle.
    fn ranked_shard(root: &std::path::Path) -> Shard {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("rank", SourceType::Long),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: rank, type: LONG, fast: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let shard = LocalIndexStore::open(root)
            .unwrap()
            .create_shard(&ShardId::single("docs"), &idx)
            .unwrap();
        let mk = |id: &str, rank: i64| {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
            let mut f = BTreeMap::new();
            f.insert("id".to_string(), Value::from(id));
            f.insert("rank".to_string(), Value::Int(rank));
            LocatedDoc {
                doc: Document::new(key, f),
                iceberg_file: "f".into(),
                row_position: 0,
            }
        };
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![mk("a", 30), mk("b", 10)],
                SourceCheckpoint::iceberg(1),
                "b1",
            ),
        )
        .unwrap();
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(vec![mk("c", 20)], SourceCheckpoint::iceberg(2), "b2"),
        )
        .unwrap();
        shard
    }

    fn id_of(hit: &SearchHit) -> String {
        let key: CompositeKey = hit.coordinates.clone().unwrap().try_into().unwrap();
        key.get("id").unwrap().to_index_string()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn search_after_cursor_round_trips_over_the_wire() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = ranked_service(tmp.path());
        let rank_asc = vec![WireSort {
            field: "rank".into(),
            descending: false,
        }];

        // Page the whole set one hit at a time, feeding next_cursor back in.
        let mut got = Vec::new();
        let mut cursor: Vec<u8> = Vec::new();
        loop {
            let resp = svc
                .search(Request::new(SearchRequest {
                    query: "rank:[0 TO 100]".into(),
                    limit: 1,
                    offset: 0,
                    sort: rank_asc.clone(),
                    search_after: cursor.clone(),
                    collapse: String::new(),
                    pit_id: 0,
                    ..Default::default()
                }))
                .await
                .unwrap()
                .into_inner();
            if resp.hits.is_empty() {
                assert!(resp.next_cursor.is_empty(), "no cursor past the end");
                break;
            }
            got.push(id_of(&resp.hits[0]));
            cursor = resp.next_cursor;
            assert!(!cursor.is_empty(), "a non-empty page yields a cursor");
        }
        // rank asc across generations: b=10, c=20, a=30.
        assert_eq!(got, vec!["b", "c", "a"]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn total_is_the_true_match_count_not_the_page_size() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = ranked_service(tmp.path());
        // 3 docs match; ask for a page of 1.
        let resp = svc
            .search(Request::new(SearchRequest {
                query: "rank:[0 TO 100]".into(),
                limit: 1,
                sort: vec![WireSort {
                    field: "rank".into(),
                    descending: false,
                }],
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.hits.len(), 1); // the page
        assert_eq!(resp.total, 3); // the true match count, not the page size
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pit_against_a_retired_shard_is_a_clean_expired_error() {
        use crate::shard_handle::ShardHandle;
        // Open a PIT, then swap in a fresh shard (as ReindexIndex does). A search carrying the old
        // pit_id must get a clean PIT-expired error, not an internal failure or stale data (task-71).
        let tmp1 = tempfile::tempdir().unwrap();
        let tmp2 = tempfile::tempdir().unwrap();
        let handle = ShardHandle::new(std::sync::Arc::new(ranked_shard(tmp1.path())));
        let svc = SearchService::new(handle.clone());

        let pit = svc
            .open_pit(Request::new(OpenPitRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(pit.pit_id != 0);

        // Retire the shard the PIT was opened on.
        handle.swap(std::sync::Arc::new(ranked_shard(tmp2.path())));

        let err = svc
            .search(Request::new(SearchRequest {
                query: "rank:[0 TO 100]".into(),
                limit: 10,
                pit_id: pit.pit_id,
                sort: vec![WireSort {
                    field: "rank".into(),
                    descending: false,
                }],
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition); // PIT_EXPIRED, not Internal
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unsorted_search_has_no_cursor_and_bad_cursor_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = ranked_service(tmp.path());

        // No sort → no cursor offered.
        let resp = svc
            .search(Request::new(SearchRequest {
                query: "rank:[0 TO 100]".into(),
                limit: 10,
                offset: 0,
                sort: Vec::new(),
                search_after: Vec::new(),
                collapse: String::new(),
                pit_id: 0,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.hits.len(), 3);
        assert!(resp.next_cursor.is_empty());

        // A malformed cursor is an InvalidArgument, not a panic.
        let err = svc
            .search(Request::new(SearchRequest {
                query: "rank:[0 TO 100]".into(),
                limit: 10,
                offset: 0,
                sort: vec![WireSort {
                    field: "rank".into(),
                    descending: false,
                }],
                search_after: b"not a cursor".to_vec(),
                collapse: String::new(),
                pit_id: 0,
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    /// A shard with a KEYWORD-fast `cat` field for collapse + a LONG-fast `rank`.
    fn collapse_service(root: &std::path::Path) -> SearchService {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("cat", SourceType::String),
                SourceField::new("rank", SourceType::Long),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: cat, type: KEYWORD, fast: true }, { path: rank, type: LONG, fast: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let shard = LocalIndexStore::open(root)
            .unwrap()
            .create_shard(&ShardId::single("docs"), &idx)
            .unwrap();
        let mk = |id: &str, cat: &str, rank: i64| {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
            let mut f = BTreeMap::new();
            f.insert("id".to_string(), Value::from(id));
            f.insert("cat".to_string(), Value::from(cat));
            f.insert("rank".to_string(), Value::Int(rank));
            LocatedDoc {
                doc: Document::new(key, f),
                iceberg_file: "f".into(),
                row_position: 0,
            }
        };
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![mk("a", "red", 30), mk("b", "blue", 10), mk("c", "red", 20)],
                SourceCheckpoint::iceberg(1),
                "b1",
            ),
        )
        .unwrap();
        SearchService::new(std::sync::Arc::new(shard))
    }

    #[tokio::test(flavor = "current_thread")]
    async fn collapse_returns_top_hit_group_and_count_over_the_wire() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = collapse_service(tmp.path());

        let resp = svc
            .search(Request::new(SearchRequest {
                query: "rank:[0 TO 100]".into(),
                limit: 10,
                offset: 0,
                sort: vec![WireSort {
                    field: "rank".into(),
                    descending: true,
                }],
                search_after: Vec::new(),
                collapse: "cat".into(),
                pit_id: 0,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        // By rank desc: a(red,30), c(red,20), b(blue,10) → groups red (top a, 2),
        // blue (top b, 1).
        let groups: Vec<(String, String, u64)> = resp
            .hits
            .iter()
            .map(|h| {
                let group = match h.group.clone().unwrap().kind.unwrap() {
                    growlerdb_proto::v1::value::Kind::Str(s) => s,
                    other => panic!("unexpected group kind: {other:?}"),
                };
                (group, id_of(h), h.group_count)
            })
            .collect();
        assert_eq!(
            groups,
            vec![
                ("red".to_string(), "a".to_string(), 2),
                ("blue".to_string(), "b".to_string(), 1),
            ]
        );
        assert!(
            resp.next_cursor.is_empty(),
            "collapse does not page by cursor"
        );

        // collapse without a sort key is rejected up front.
        let err = svc
            .search(Request::new(SearchRequest {
                query: "rank:[0 TO 100]".into(),
                limit: 10,
                offset: 0,
                sort: Vec::new(),
                search_after: Vec::new(),
                collapse: "cat".into(),
                pit_id: 0,
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn search_on_an_unknown_field_is_invalid_argument_not_internal() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = ranked_service(tmp.path());
        // `body` isn't in the schema (id, rank) — a client typo, so it's a 400/InvalidArgument
        // (the shard's `UnknownField` mapped via store_status), not an opaque 500/Internal.
        let err = svc
            .search(Request::new(SearchRequest {
                query: "body:hello".into(),
                limit: 10,
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(
            err.message().contains("body"),
            "message should name the bad field: {}",
            err.message()
        );
    }

    #[tokio::test]
    async fn oversized_page_fetch_is_rejected_at_the_node() {
        // task-146 / F13: a direct RPC with a giant limit must be rejected before building the page,
        // not left to OOM the Node (the Gateway isn't in the path for a direct Node RPC).
        let tmp = tempfile::tempdir().unwrap();
        let svc = ranked_service(tmp.path());
        let err = svc
            .search(Request::new(SearchRequest {
                query: "*".into(),
                limit: u32::MAX,
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(err.message().contains("maximum page fetch"));
    }

    /// Build a `rank`-sorted shard (a@30, b@10 in gen 1; c@20 in gen 2) and a service
    /// over it, returning both so the test can commit further writes.
    fn service_and_shard(root: &std::path::Path) -> (SearchService, Arc<Shard>) {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("rank", SourceType::Long),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: rank, type: LONG, fast: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let shard = LocalIndexStore::open(root)
            .unwrap()
            .create_shard(&ShardId::single("docs"), &idx)
            .unwrap();
        write_doc(&shard, "a", 30, 1, "b1a");
        write_doc(&shard, "b", 10, 1, "b1b");
        write_doc(&shard, "c", 20, 2, "b2c");
        let shard = Arc::new(shard);
        (SearchService::new(shard.clone()), shard)
    }

    fn write_doc(shard: &Shard, id: &str, rank: i64, snap: i64, batch: &str) {
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from(id));
        f.insert("rank".to_string(), Value::Int(rank));
        let doc = LocatedDoc {
            doc: Document::new(key, f),
            iceberg_file: "f".into(),
            row_position: 0,
        };
        IndexWriter::write(
            shard,
            &CommitBatch::from_upserts(vec![doc], SourceCheckpoint::iceberg(snap), batch),
        )
        .unwrap();
    }

    fn rank_asc() -> Vec<WireSort> {
        vec![WireSort {
            field: "rank".into(),
            descending: false,
        }]
    }

    fn search_req(sort: Vec<WireSort>, pit_id: u64) -> SearchRequest {
        SearchRequest {
            query: "rank:[0 TO 1000]".into(),
            limit: 10,
            offset: 0,
            sort,
            search_after: Vec::new(),
            collapse: String::new(),
            pit_id,
            ..Default::default()
        }
    }

    /// A PIT opened over the wire freezes the snapshot: a search carrying its `pit_id`
    /// ignores a later superseding commit, while a fresh search reflects it.
    #[tokio::test(flavor = "current_thread")]
    async fn pit_search_is_consistent_over_the_wire() {
        let tmp = tempfile::tempdir().unwrap();
        let (svc, shard) = service_and_shard(tmp.path());

        let open = svc
            .open_pit(Request::new(OpenPitRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(open.pit_id != 0);

        // After the PIT: a→5 (supersede) and a new d@40.
        write_doc(&shard, "a", 5, 3, "b3a");
        write_doc(&shard, "d", 40, 3, "b3d");

        // PIT search sees the as-of-open world: rank asc → b(10), c(20), a(30).
        let pit_resp = svc
            .search(Request::new(search_req(rank_asc(), open.pit_id)))
            .await
            .unwrap()
            .into_inner();
        let pit_ids: Vec<String> = pit_resp.hits.iter().map(id_of).collect();
        assert_eq!(pit_ids, vec!["b", "c", "a"]);

        // A fresh search reflects the new state: a(5), b(10), c(20), d(40).
        let fresh = svc
            .search(Request::new(search_req(rank_asc(), 0)))
            .await
            .unwrap()
            .into_inner();
        let fresh_ids: Vec<String> = fresh.hits.iter().map(id_of).collect();
        assert_eq!(fresh_ids, vec!["a", "b", "c", "d"]);

        // Closing the PIT, then using it, is a clear FailedPrecondition.
        let closed = svc
            .close_pit(Request::new(ClosePitRequest {
                pit_id: open.pit_id,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(closed.closed);
        let err = svc
            .search(Request::new(search_req(rank_asc(), open.pit_id)))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
    }

    /// Export streams the whole result set in keyset order across pages, and pins a
    /// snapshot (a self-opened PIT) so a concurrent write can't perturb the scroll.
    #[tokio::test(flavor = "current_thread")]
    async fn export_streams_full_result_set_in_pages() {
        use tokio_stream::StreamExt;
        let tmp = tempfile::tempdir().unwrap();
        let (svc, _shard) = service_and_shard(tmp.path());

        let mut stream = svc
            .export(Request::new(ExportRequest {
                query: "rank:[0 TO 1000]".into(),
                page_size: 2,
                sort: rank_asc(),
                pit_id: 0, // server opens + closes a PIT for the export
            }))
            .await
            .unwrap()
            .into_inner();

        let mut got = Vec::new();
        let mut pages = 0;
        while let Some(page) = stream.next().await {
            let page = page.unwrap();
            pages += 1;
            got.extend(page.hits.iter().map(id_of));
        }
        // rank asc across generations: b(10), c(20), a(30); page_size 2 → 2 pages.
        assert_eq!(got, vec!["b", "c", "a"]);
        assert_eq!(pages, 2);

        // The self-opened PIT was closed at the end of the stream.
        assert_eq!(svc.shard.current().open_pit_count(), 0);

        // Export requires a sort (it defines the scroll order).
        let err = svc
            .export(Request::new(ExportRequest {
                query: "rank:[0 TO 1000]".into(),
                page_size: 0,
                sort: Vec::new(),
                pit_id: 0,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    /// The auth hook gates every RPC: a denying hook rejects a Search before it runs.
    #[tokio::test(flavor = "current_thread")]
    async fn auth_hook_gates_the_search_rpc() {
        use crate::auth::{AuthContext, AuthDenied, AuthHook};

        struct DenyAll;
        impl AuthHook for DenyAll {
            fn authorize(&self, _ctx: &AuthContext) -> Result<(), AuthDenied> {
                Err(AuthDenied::new("nope"))
            }
        }

        let tmp = tempfile::tempdir().unwrap();
        let (allow, shard) = service_and_shard(tmp.path());
        let deny = SearchService::with_auth(shard.clone(), std::sync::Arc::new(DenyAll));

        // The denying hook rejects with PermissionDenied...
        let err = deny
            .search(Request::new(search_req(rank_asc(), 0)))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::PermissionDenied);

        // ...while the default (AllowAll) service serves the same request fine.
        assert!(allow
            .search(Request::new(search_req(rank_asc(), 0)))
            .await
            .is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn aggregate_runs_a_stats_agg_over_a_fast_field() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = ranked_service(tmp.path()); // rank: a=30, b=10, c=20

        let resp = svc
            .aggregate(Request::new(AggregateRequest {
                query: "rank:[0 TO 100]".into(),
                aggs: r#"{"rank_stats": {"Stats": {"field": "rank"}}}"#.into(),
                partial: false,
                window: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        let results: serde_json::Value = serde_json::from_str(&resp.results).unwrap();
        let stats = &results["rank_stats"];
        assert_eq!(stats["count"].as_u64().unwrap(), 3);
        assert_eq!(stats["min"].as_f64().unwrap(), 10.0);
        assert_eq!(stats["max"].as_f64().unwrap(), 30.0);
        assert_eq!(stats["sum"].as_f64().unwrap(), 60.0);
        assert_eq!(stats["avg"].as_f64().unwrap(), 20.0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn aggregate_rejects_a_malformed_spec() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = ranked_service(tmp.path());
        let err = svc
            .aggregate(Request::new(AggregateRequest {
                query: "rank:[0 TO 100]".into(),
                aggs: "{not valid".into(),
                partial: false,
                window: 0,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    // ---- Tenant scoping (task-38) -----------------------------------------------------

    /// A tenant-scoped shard: a `tenant` KEYWORD field declared as `tenant_field`, with docs
    /// from two tenants (`acme`: a, c; `globex`: b).
    fn tenant_service(root: &std::path::Path) -> SearchService {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("tenant", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\ntenant_field: tenant\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: tenant, type: KEYWORD } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let shard = LocalIndexStore::open(root)
            .unwrap()
            .create_shard(&ShardId::single("docs"), &idx)
            .unwrap();
        let mk = |id: &str, tenant: &str| {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
            let mut f = BTreeMap::new();
            f.insert("id".to_string(), Value::from(id));
            f.insert("tenant".to_string(), Value::from(tenant));
            LocatedDoc {
                doc: Document::new(key, f),
                iceberg_file: "f".into(),
                row_position: 0,
            }
        };
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![mk("a", "acme"), mk("b", "globex"), mk("c", "acme")],
                SourceCheckpoint::iceberg(1),
                "b1",
            ),
        )
        .unwrap();
        SearchService::new(Arc::new(shard))
    }

    fn tenant_req(query: &str, tenant: Option<&str>) -> Request<SearchRequest> {
        let mut req = Request::new(SearchRequest {
            query: query.into(),
            limit: 10,
            ..Default::default()
        });
        if let Some(t) = tenant {
            req.metadata_mut()
                .insert("x-growlerdb-tenant", t.parse().unwrap());
        }
        req
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tenant_scoped_search_returns_only_the_callers_tenant() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = tenant_service(tmp.path());
        let resp = svc
            .search(tenant_req("id:a OR id:b OR id:c", Some("acme")))
            .await
            .unwrap()
            .into_inner();
        let mut ids: Vec<String> = resp.hits.iter().map(id_of).collect();
        ids.sort();
        // `b` belongs to globex and is filtered out, though the query matched it.
        assert_eq!(ids, vec!["a", "c"]);
        assert_eq!(resp.total, 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_tenant_query_clause_cannot_widen_past_the_injected_filter() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = tenant_service(tmp.path());
        // acme tries to OR in globex's rows; the mandatory AND tenant:acme still binds.
        let resp = svc
            .search(tenant_req("tenant:globex OR id:a", Some("acme")))
            .await
            .unwrap()
            .into_inner();
        let ids: Vec<String> = resp.hits.iter().map(id_of).collect();
        assert_eq!(ids, vec!["a"]); // globex's `b` never leaks
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_tenant_scoped_index_requires_a_verified_claim() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = tenant_service(tmp.path());
        let err = svc.search(tenant_req("id:a", None)).await.unwrap_err();
        assert_eq!(err.code(), Code::PermissionDenied);
    }
}
