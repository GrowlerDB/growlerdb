//! The Node **Lookup** gRPC service — the PK-lookup / hydration path: resolve each
//! coordinate through the shard [locator](growlerdb_core::RowLocator) and read the
//! authoritative rows from Iceberg. Pairs with [`SearchService`] (which returns
//! coordinates) to complete a search → row round-trip.
//!
//! [`SearchService`]: crate::SearchService

use growlerdb_core::{CompositeKey, HydratedRow, Projection, ResolvedIndex};
use growlerdb_proto::v1::{
    Error as WireError, Field as WireField, GetByKeyRequest, GetByKeyResponse,
    HydratedRow as WireRow,
};
use growlerdb_proto::{to_status, Lookup, LookupServer};
use growlerdb_source::{IcebergConfig, SharedReader};
use std::sync::Arc;
use tonic::{Code, Request, Response, Status};

use crate::auth::{self, default_auth, SharedAuth};
use crate::error::EngineError;
use crate::hydrate;
use crate::shard_handle::ShardHandle;

/// Hard ceiling on coordinates per `GetByKey`: a directly-reachable Node must self-defend
/// against an unbounded batch that would pin locate/hydrate work. Enforced at the Gateway too.
pub(crate) const MAX_KEYS: usize = 1_000;

/// A `Lookup` service over one shard and its Iceberg source. Every RPC consults the
/// [auth hook](SharedAuth) (no-op by default) first.
///
/// The Iceberg source is a shared, lazily-connected [`SharedReader`]: the catalog client is
/// built once and reused across RPCs (with the reader's snapshot-pinned plan cache); a source
/// failure invalidates it so the next RPC reconnects.
#[derive(Clone)]
pub struct LookupService {
    shard: ShardHandle,
    reader: Arc<SharedReader>,
    table: String,
    /// Carried so hydration can fork a variant table onto the interim Trino lane (D48/D49):
    /// `TrinoHydrator::hydrate` needs the full `ResolvedIndex`, which the `Shard` doesn't retain.
    resolved: Arc<ResolvedIndex>,
    auth: SharedAuth,
}

impl LookupService {
    /// A Lookup service hydrating `shard`'s coordinates from `table` in catalog `iceberg`, with
    /// the default no-op auth hook. `resolved` is the served index (drives the variant fork).
    pub fn new(
        shard: impl Into<ShardHandle>,
        iceberg: IcebergConfig,
        table: impl Into<String>,
        resolved: ResolvedIndex,
    ) -> Self {
        Self::with_auth(shard, iceberg, table, resolved, default_auth())
    }

    /// As [`new`](Self::new), with a specific [auth hook](SharedAuth).
    pub fn with_auth(
        shard: impl Into<ShardHandle>,
        iceberg: IcebergConfig,
        table: impl Into<String>,
        resolved: ResolvedIndex,
        auth: SharedAuth,
    ) -> Self {
        Self {
            shard: shard.into(),
            reader: Arc::new(SharedReader::new(iceberg)),
            table: table.into(),
            resolved: Arc::new(resolved),
            auth,
        }
    }

    /// Wrap as a mountable tonic [`LookupServer`].
    pub fn into_server(self) -> LookupServer<Self> {
        LookupServer::new(self)
    }
}

#[tonic::async_trait]
impl Lookup for LookupService {
    #[tracing::instrument(name = "node.hydrate", skip_all, err)]
    async fn get_by_key(
        &self,
        request: Request<GetByKeyRequest>,
    ) -> Result<Response<GetByKeyResponse>, Status> {
        let started = std::time::Instant::now();
        auth::authorize(&self.auth, "GetByKey", &request)?;
        let tenant = auth::tenant_of(&request);
        let req = request.into_inner();

        // Bound the batch before any locate/hydrate: an unbounded key list would build an
        // arbitrarily large working set from a single request.
        if req.keys.len() > MAX_KEYS {
            return Err(to_status(
                Code::InvalidArgument,
                WireError::new(
                    "INVALID_ARGUMENT",
                    format!("keys ({}) exceeds the maximum ({MAX_KEYS})", req.keys.len()),
                ),
            ));
        }

        let mut keys: Vec<CompositeKey> = Vec::with_capacity(req.keys.len());
        for coord in req.keys {
            let key = coord.try_into().map_err(|e| {
                to_status(
                    Code::InvalidArgument,
                    WireError::new("INVALID_ARGUMENT", format!("bad coordinates: {e}")),
                )
            })?;
            keys.push(key);
        }

        // Pin the live shard for this request (a concurrent reindex swap won't pull it).
        let shard = self.shard.current();

        // On a tenant-scoped index a caller must carry a verified claim; enforcement happens
        // *after* hydration against the row's authoritative tenant value (the field is
        // decoupled from the key, so a forged coordinate can't be caught before the read).
        let tenant_field = shard.tenant_field().map(str::to_string);
        if let Some(field) = &tenant_field {
            if tenant.is_none() {
                return Err(to_status(
                    Code::PermissionDenied,
                    WireError::new(
                        "PERMISSION_DENIED",
                        format!(
                            "index is tenant-scoped on `{field}`; request carries no verified tenant"
                        ),
                    ),
                ));
            }
        }

        // On a scoped index, force-fetch the tenant field (so we can verify it) and remember to
        // strip it afterward if the caller didn't ask for it.
        let mut added_tenant_col = false;
        let projection = if req.columns.is_empty() {
            Projection::All
        } else {
            let mut cols = req.columns;
            if let Some(field) = &tenant_field {
                if !cols.iter().any(|c| c == field) {
                    cols.push(field.clone());
                    added_tenant_col = true;
                }
            }
            Projection::Columns(cols)
        };

        // Resolve first (a quick, Iceberg-free local read) so a missing key is a clear `NotFound`
        // *before* we connect to the catalog. Strategy-aware: `COORDINATES` uses the layered
        // locate + live-file bitmap; `PREDICATE` a locator-less key-presence check.
        let predicate_index =
            shard.location_strategy() == growlerdb_core::LocationStrategy::Predicate;
        let located = hydrate::resolve_requests(&shard, &keys).map_err(engine_status)?;
        growlerdb_telemetry::sli::locate_keys(located.len() as u64);

        // Variant fork (D48/D49): a variant-table index hydrates through the interim Trino lane
        // (released iceberg-rust can't scan a v3 variant table). Non-variant indexes read the
        // projected rows via the native `SharedReader`, dropping it on a source failure so the
        // next RPC reconnects.
        let result = if self.resolved.has_variant_field() {
            growlerdb_source::shared_hydrator()
                .hydrate(&self.resolved, &located, &projection)
                .await
                .map_err(|e| engine_status(EngineError::Source(e)))?
        } else {
            let reader = match self.reader.get().await {
                Ok(reader) => reader,
                Err(e) => return Err(engine_status(EngineError::Source(e))),
            };
            match reader.hydrate(&self.table, &located, &projection).await {
                Ok(result) => result,
                Err(e) => {
                    self.reader.invalidate().await;
                    return Err(engine_status(EngineError::Source(e)));
                }
            }
        };
        if let Some(hit) = result.plan_cache_hit {
            growlerdb_telemetry::sli::plan_cache(hit);
        }
        // `requested - found` is the hydration-miss signal: a stale index (e.g. a recreated
        // source) returns hits whose keys no longer exist in the table. On a `PREDICATE` index the
        // pruned key scan is the *primary* read path, not a refresh, so its re-found rows never
        // count as stale (`growlerdb_stale_locators_total` stays 0) and there's no layer to write back.
        let refreshed = stale_locators_for_metrics(predicate_index, result.refreshed.len());
        let requested = keys.len() as u64;
        let found = result.rows.len() as u64;
        // The key scan saw >1 distinct source row for a key.
        growlerdb_telemetry::sli::duplicate_pks(result.duplicate_pks);
        if !predicate_index {
            shard
                .refresh_locators(&result.refreshed)
                .map_err(|e| engine_status(EngineError::Store(e)))?;
        }

        let mut rows = result.rows;
        if let (Some(field), Some(claim)) = (&tenant_field, &tenant) {
            enforce_tenant_post_hydration(&mut rows, field, claim, added_tenant_col);
        }

        // Hydration SLI: latency + stale-locator count + requested-vs-found (index↔source drift).
        growlerdb_telemetry::sli::hydration(
            &self.resolved.name,
            started.elapsed().as_secs_f64(),
            refreshed,
            requested,
            found,
        );
        Ok(Response::new(GetByKeyResponse {
            rows: rows.into_iter().map(to_wire_row).collect(),
            failed_shards: 0, // a Node serves one shard; the Gateway sets this on merge
        }))
    }
}

/// Tenant post-filter: drop every hydrated row whose authoritative `tenant_field` value isn't the
/// caller's verified `claim`, so a forged coordinate can never hydrate another tenant's row. Drops
/// are silent (not errors), so the caller can't confirm a foreign row exists. When `added_tenant_col`
/// (the column was force-fetched only to verify), it's stripped from the surviving rows.
fn enforce_tenant_post_hydration(
    rows: &mut Vec<HydratedRow>,
    field: &str,
    claim: &str,
    added_tenant_col: bool,
) {
    rows.retain(|r| r.fields.get(field).map(|v| v.to_index_string()).as_deref() == Some(claim));
    if added_tenant_col {
        for r in rows {
            r.fields.remove(field);
        }
    }
}

/// The stale-locator count for the hydration SLI. On a `PREDICATE` index re-found rows came
/// through the pruned key scan (the primary read path, not a refresh), so none count. On
/// `COORDINATES` a re-found row means a locator had genuinely gone stale, so each one counts.
fn stale_locators_for_metrics(predicate_index: bool, refound_rows: usize) -> u64 {
    if predicate_index {
        0
    } else {
        refound_rows as u64
    }
}

/// A hydrated row → its wire form.
fn to_wire_row(row: HydratedRow) -> WireRow {
    WireRow {
        key: Some((&row.key).into()),
        fields: row
            .fields
            .into_iter()
            .map(|(name, value)| WireField {
                name,
                value: Some(value.into()),
            })
            .collect(),
    }
}

/// Map an engine error to a gRPC status: a key with no locator is a client-facing
/// `NotFound`; everything else is `Internal`.
fn engine_status(e: EngineError) -> Status {
    match e {
        EngineError::MissingLocator(_) => {
            to_status(Code::NotFound, WireError::new("NOT_FOUND", e.to_string()))
        }
        other => to_status(
            Code::Internal,
            WireError::new("INTERNAL", other.to_string()),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{AuthContext, AuthDenied, AuthHook};
    use growlerdb_core::{
        CommitBatch, Document, IndexDefinition, IndexWriter, LocatedDoc, SourceCheckpoint,
        SourceField, SourceSchema, SourceType, Value,
    };
    use growlerdb_index::{LocalIndexStore, ShardId};
    use growlerdb_proto::v1::Coordinates;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn service(root: &std::path::Path, auth: SharedAuth) -> LookupService {
        let src = SourceSchema::new(
            vec![SourceField::new("id", SourceType::String)],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let shard = LocalIndexStore::open(root)
            .unwrap()
            .create_shard(&ShardId::single("docs"), &idx)
            .unwrap();
        // Index one doc so the index exists (its locator resolves; "missing" won't).
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from("present"))]);
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from("present"));
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![LocatedDoc {
                    doc: Document::new(key, f),
                    iceberg_file: "data/f0.parquet".into(),
                    row_position: 0,
                }],
                SourceCheckpoint::iceberg(1),
                "b1",
            ),
        )
        .unwrap();
        LookupService::with_auth(
            Arc::new(shard),
            IcebergConfig::local(),
            "g.docs",
            crate::test_docs_resolved(),
            auth,
        )
    }

    fn coord(id: &str) -> Coordinates {
        (&CompositeKey::new(vec![], vec![("id".into(), Value::from(id))])).into()
    }

    struct DenyAll;
    impl AuthHook for DenyAll {
        fn authorize(&self, _ctx: &AuthContext) -> Result<(), AuthDenied> {
            Err(AuthDenied::new("nope"))
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn auth_denial_is_rejected_before_any_hydration() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path(), Arc::new(DenyAll));
        let err = svc
            .get_by_key(Request::new(GetByKeyRequest {
                shard: 0,
                window: 0,
                keys: vec![coord("present")],
                columns: vec![],
                index: String::new(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::PermissionDenied);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unknown_key_is_not_found_before_connecting_to_iceberg() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path(), default_auth());
        // "missing" was never indexed → no locator → NotFound, resolved without ever
        // reaching the (absent) Iceberg catalog.
        let err = svc
            .get_by_key(Request::new(GetByKeyRequest {
                shard: 0,
                window: 0,
                keys: vec![coord("missing")],
                columns: vec![],
                index: String::new(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn malformed_coordinates_are_invalid_argument() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path(), default_auth());
        // A Field with no value can't decode into a CompositeKey.
        let bad = Coordinates {
            partition: vec![],
            identifier: vec![WireField {
                name: "id".into(),
                value: None,
            }],
        };
        let err = svc
            .get_by_key(Request::new(GetByKeyRequest {
                shard: 0,
                window: 0,
                keys: vec![bad],
                columns: vec![],
                index: String::new(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn over_limit_batch_is_rejected_before_any_locate() {
        let tmp = tempfile::tempdir().unwrap();
        // DenyAll would fire first, so use the default (allow) auth to prove the size cap gates.
        let svc = service(tmp.path(), default_auth());
        let keys = (0..=MAX_KEYS).map(|i| coord(&i.to_string())).collect();
        let err = svc
            .get_by_key(Request::new(GetByKeyRequest {
                shard: 0,
                window: 0,
                keys,
                columns: vec![],
                index: String::new(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    /// A tenant-scoped lookup service (tenant_field `tenant`), one indexed doc.
    fn tenant_service(root: &std::path::Path) -> LookupService {
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
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from("present"))]);
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from("present"));
        f.insert("tenant".to_string(), Value::from("acme"));
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![LocatedDoc {
                    doc: Document::new(key, f),
                    iceberg_file: "data/f0.parquet".into(),
                    row_position: 0,
                }],
                SourceCheckpoint::iceberg(1),
                "b1",
            ),
        )
        .unwrap();
        LookupService::new(
            Arc::new(shard),
            IcebergConfig::local(),
            "g.docs",
            crate::test_docs_resolved(),
        )
    }

    /// As [`service`], but the index uses the **PREDICATE** location strategy.
    fn predicate_service(root: &std::path::Path) -> LookupService {
        let src = SourceSchema::new(
            vec![SourceField::new("id", SourceType::String)],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nlocation_strategy: PREDICATE\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let shard = LocalIndexStore::open(root)
            .unwrap()
            .create_shard(&ShardId::single("docs"), &idx)
            .unwrap();
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from("present"))]);
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from("present"));
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![LocatedDoc {
                    doc: Document::new(key, f),
                    iceberg_file: "data/f0.parquet".into(),
                    row_position: 0,
                }],
                SourceCheckpoint::iceberg(1),
                "b1",
            ),
        )
        .unwrap();
        LookupService::new(
            Arc::new(shard),
            IcebergConfig::local(),
            "g.docs",
            crate::test_docs_resolved(),
        )
    }

    #[tokio::test(flavor = "current_thread")]
    async fn predicate_index_unknown_key_is_not_found_before_connecting_to_iceberg() {
        // The NotFound-before-catalog contract holds under PREDICATE too: presence is a
        // local key-term probe (there is no locator to resolve), so a missing key never
        // costs an Iceberg connect — and never triggers a broad scan.
        let tmp = tempfile::tempdir().unwrap();
        let svc = predicate_service(tmp.path());
        let err = svc
            .get_by_key(Request::new(GetByKeyRequest {
                shard: 0,
                window: 0,
                keys: vec![coord("missing")],
                columns: vec![],
                index: String::new(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound);
    }

    #[test]
    fn predicate_hydration_never_counts_stale_locators() {
        // A `PREDICATE` hydration re-finds every key through the pruned scan — that's
        // its primary path, not a refresh, so `growlerdb_stale_locators_total` must not
        // move. Under `COORDINATES` the same re-found rows are genuinely stale locators.
        assert_eq!(stale_locators_for_metrics(true, 5), 0);
        assert_eq!(stale_locators_for_metrics(true, 0), 0);
        assert_eq!(stale_locators_for_metrics(false, 5), 5);
        assert_eq!(stale_locators_for_metrics(false, 0), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tenant_scoped_hydration_requires_a_claim_before_any_iceberg_connect() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = tenant_service(tmp.path());
        // No `x-growlerdb-tenant` metadata → fail closed with PermissionDenied, *before*
        // resolving locators or reaching the (absent) catalog.
        let err = svc
            .get_by_key(Request::new(GetByKeyRequest {
                shard: 0,
                window: 0,
                keys: vec![coord("present")],
                columns: vec![],
                index: String::new(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::PermissionDenied);
    }

    /// A hydrated row carrying an authoritative tenant value.
    fn hydrated(id: &str, tenant: &str) -> HydratedRow {
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
        let mut fields = BTreeMap::new();
        fields.insert("id".to_string(), Value::from(id));
        fields.insert("tenant".to_string(), Value::from(tenant));
        HydratedRow { key, fields }
    }

    /// Rows are dropped unless their real `tenant` value matches the caller's claim; here we prove
    /// the filter on hydrated rows without a live catalog (`tenant_isolation.rs` covers end-to-end).
    #[test]
    fn post_hydration_filter_drops_foreign_tenant_rows() {
        // acme asked for its own row `a` and (via a forged coordinate) globex's `b`; both hydrate,
        // but only acme's survives the authoritative-value filter.
        let mut rows = vec![hydrated("a", "acme"), hydrated("b", "globex")];
        enforce_tenant_post_hydration(&mut rows, "tenant", "acme", false);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].fields["id"].to_index_string(), "a");
        // Not force-fetched, so the tenant column stays on the surviving row.
        assert_eq!(rows[0].fields["tenant"].to_index_string(), "acme");
    }

    #[test]
    fn post_hydration_filter_strips_the_added_tenant_column() {
        // When the tenant column was added to the projection only to enforce the filter, it's removed
        // from the surviving rows so the caller sees exactly the columns it requested.
        let mut rows = vec![hydrated("a", "acme"), hydrated("b", "globex")];
        enforce_tenant_post_hydration(&mut rows, "tenant", "acme", true);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].fields["id"].to_index_string(), "a");
        assert!(
            !rows[0].fields.contains_key("tenant"),
            "added column stripped"
        );
    }

    #[test]
    fn post_hydration_filter_drops_everything_for_a_foreign_only_result() {
        // A coordinate that resolves solely to another tenant's row yields an empty result — the
        // caller can't even confirm the foreign row exists.
        let mut rows = vec![hydrated("b", "globex")];
        enforce_tenant_post_hydration(&mut rows, "tenant", "acme", false);
        assert!(rows.is_empty());
    }
}
