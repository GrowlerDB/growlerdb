//! `growlerdb-telemetry` — the **observability foundation**: structured JSON
//! logging, a Prometheus metrics recorder for the core SLIs, and Kubernetes health/readiness
//! probes. Per [observability](../../../wiki/23-observability.md), telemetry is open and
//! standards-based: metrics are scrapeable in Prometheus exposition format (the wiki's
//! "scrape or OTLP" metrics path), logs are machine-parseable JSON carrying span context.
//!
//! Metrics travel the Prometheus scrape path (`/metrics`); traces are emitted as `tracing`
//! spans and, when `GROWLERDB_OTLP_ENDPOINT` is set, **exported via OTLP** (HTTP/protobuf, over
//! the in-tree `reqwest`) to a collector (Tempo/Jaeger/any). Without that env var the spans
//! stay local — visible as span context in the JSON logs. The `sli` module names the core SLIs.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{SpanExporter, WithExportConfig as _};
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::{EnvFilter, Layer};

/// The installed Prometheus recorder handle, used to render exposition text on `/metrics`.
static PROMETHEUS: OnceLock<PrometheusHandle> = OnceLock::new();
/// The OTLP tracer provider, kept alive to flush spans on [`shutdown`]. Set only when OTLP
/// export is configured.
static TRACER_PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();

/// Env var naming the OTLP/HTTP collector endpoint (e.g. `http://localhost:4318`). When unset,
/// trace export is off.
const OTLP_ENDPOINT_ENV: &str = "GROWLERDB_OTLP_ENDPOINT";

/// Initialize process telemetry: a global Prometheus metrics recorder + structured JSON logging
/// (the `tracing` subscriber, filtered by `GROWLERDB_LOG` / `RUST_LOG`, default `info`), and —
/// when [`OTLP_ENDPOINT_ENV`] is set — an OTLP span exporter to that collector. **Idempotent**:
/// safe to call once per process; later calls (and a pre-existing global subscriber, e.g. in
/// tests) are no-ops rather than panics. `service` tags every log and the trace resource.
pub fn init(service: &str) {
    // Global metrics recorder (install once). The handle renders Prometheus exposition text.
    //
    // Give the latency histograms explicit buckets: without them, the Prometheus exporter
    // renders a `histogram!` as a **summary** (client-computed `{quantile}` series over a decaying
    // window), which has no `_bucket` — so `histogram_quantile` returns nothing and the reported
    // p50/p95/p99 drift on every scrape and decay to 0 when traffic stops (confusing on a live chart).
    // With buckets it exports a true cumulative histogram: stable, monotonic, aggregatable across
    // replicas, and queryable with `histogram_quantile(…, rate(…_bucket[5m]))`. All latency metrics end
    // in `_duration_seconds`, so one suffix rule covers query / hydration / http-request.
    if PROMETHEUS.get().is_none() {
        const LATENCY_BUCKETS: &[f64] = &[
            0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ];
        let builder = PrometheusBuilder::new()
            .set_buckets_for_metric(Matcher::Suffix("_duration_seconds".into()), LATENCY_BUCKETS)
            .expect("latency buckets are non-empty");
        if let Ok(handle) = builder.install_recorder() {
            let _ = PROMETHEUS.set(handle);
        }
    }

    let filter = EnvFilter::try_from_env("GROWLERDB_LOG")
        .or_else(|_| EnvFilter::try_from_default_env())
        .unwrap_or_else(|_| EnvFilter::new("info"));
    let json_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_current_span(true)
        .with_span_list(false)
        .with_target(true);
    // `try_init` doesn't panic if a global subscriber is already set (test processes, double init).
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(json_layer)
        .with(otlp_layer(service))
        .try_init();
    tracing::info!(service, "telemetry initialized");
}

/// Build the OTLP trace-export layer when [`OTLP_ENDPOINT_ENV`] is set, else `None` (a `None`
/// layer is a no-op). Stores the provider so [`shutdown`] can flush. Generic over the
/// subscriber so it composes into the layered registry.
fn otlp_layer<S>(service: &str) -> Option<Box<dyn Layer<S> + Send + Sync>>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
{
    let endpoint = std::env::var(OTLP_ENDPOINT_ENV).ok()?;
    let exporter = SpanExporter::builder()
        .with_http()
        .with_endpoint(traces_endpoint(&endpoint))
        .build()
        .ok()?;
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder()
                .with_service_name(service.to_string())
                .build(),
        )
        .build();
    let tracer = provider.tracer("growlerdb");
    let _ = TRACER_PROVIDER.set(provider);
    Some(tracing_opentelemetry::layer().with_tracer(tracer).boxed())
}

/// The OTLP `GROWLERDB_OTLP_ENDPOINT` env names the collector **base** (e.g.
/// `http://lgtm:4318`), but `SpanExporter::with_endpoint` uses its value **verbatim** — it does
/// NOT append the per-signal path — so the spans must be POSTed to `…/v1/traces`. Append it
/// here (idempotently). Without this, the exporter hits the base URL and the collector 404s.
fn traces_endpoint(base: &str) -> String {
    let base = base.trim_end_matches('/');
    if base.ends_with("/v1/traces") {
        base.to_string()
    } else {
        format!("{base}/v1/traces")
    }
}

/// Flush any buffered OTLP spans and shut the exporter down. Call before process exit so a
/// graceful shutdown doesn't drop the last batch of traces. A no-op when OTLP export is off.
pub fn shutdown() {
    if let Some(provider) = TRACER_PROVIDER.get() {
        let _ = provider.shutdown();
    }
}

/// Render the current metrics in Prometheus exposition format (empty before [`init`]).
pub fn metrics_text() -> String {
    PROMETHEUS
        .get()
        .map(PrometheusHandle::render)
        .unwrap_or_default()
}

/// A shared readiness flag for the `/readyz` probe: a component flips it once it is warm
/// enough to serve (a Gateway with its shards connected, a Node with its shard restored).
/// Liveness (`/healthz`) is always OK while the process runs; readiness gates traffic.
#[derive(Clone, Default)]
pub struct Readiness(Arc<AtomicBool>);

impl Readiness {
    /// A not-yet-ready flag.
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark the component ready to serve traffic.
    pub fn mark_ready(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    /// Whether the component has signalled readiness.
    pub fn is_ready(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// An axum router exposing the operational endpoints for Kubernetes + scraping:
/// `GET /healthz` (liveness, always 200 while up), `GET /readyz` (200 ready / 503 not), and
/// `GET /metrics` (Prometheus exposition). Mount on a dedicated telemetry port.
pub fn health_router(readiness: Readiness) -> Router {
    Router::new()
        .route("/healthz", get(|| async { (StatusCode::OK, "ok") }))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_handler))
        .with_state(readiness)
}

async fn readyz(State(readiness): State<Readiness>) -> (StatusCode, &'static str) {
    if readiness.is_ready() {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}

async fn metrics_handler() -> (StatusCode, String) {
    (StatusCode::OK, metrics_text())
}

/// The first-class **SLIs** ([observability](../../../wiki/23-observability.md)) the engine
/// records through, so metric names stay consistent across call sites. RED for requests
/// (rate/errors/duration), throughput/lag for ingestion.
pub mod sli {
    use metrics::{counter, gauge, histogram};

    /// Record an index's **ingestion lag** — the worst shard's wall-clock staleness vs the source
    /// head, in ms. A gauge (current value, not a rate), labelled by `index` so the
    /// console can show a per-index "behind by Ns" panel and Grafana/alerts can threshold it.
    /// 0 means every shard is caught up. Emitted whenever ingestion status is computed.
    pub fn ingest_lag_ms(index: &str, lag_ms: i64) {
        gauge!("growlerdb_ingest_lag_ms", "index" => index.to_string()).set(lag_ms as f64);
    }

    /// Record an index's **shard availability**: `up` = shards with a reachable primary,
    /// `total` = shards the index has. Two gauges labelled by `index` so the console shows an "N/M
    /// shards up" panel and an alert can fire when `up < total`.
    pub fn shard_availability(index: &str, up: u64, total: u64) {
        gauge!("growlerdb_shards_up", "index" => index.to_string()).set(up as f64);
        gauge!("growlerdb_shards_total", "index" => index.to_string()).set(total as f64);
    }

    /// Record the current **live segment count** for an index/shard — the merge health
    /// signal a segments panel charts. A gauge (current value), labelled by `label` (the index name,
    /// or `"<index> w<window>"` for a windowed shard). Emit it on the compaction loop's tick so it
    /// tracks segment growth between merges.
    pub fn segments_live(label: &str, segments: u64) {
        gauge!("growlerdb_segments_live", "index" => label.to_string()).set(segments as f64);
    }

    /// A shard's full on-disk index footprint in bytes — Tantivy files **plus** the
    /// locator layers, i.e. exactly the sum of `growlerdb_index_bytes_component`.
    /// Emitted on the compaction loop's tick alongside `segments_live` — so the index-size panel
    /// tracks growth (and the drop after a merge), not just the segment count.
    pub fn index_bytes(label: &str, bytes: u64) {
        gauge!("growlerdb_index_bytes", "index" => label.to_string()).set(bytes as f64);
    }

    /// Live indexed document count for a shard, emitted on the compaction loop's tick
    /// alongside `index_bytes`. `sum(growlerdb_index_docs)` across shards is GrowlerDB's own count of
    /// what it holds — the index side of the **source→index convergence** check: at steady state it
    /// must equal the source's live row count (`sum(growlerdb_source_records)`); a persistent gap is
    /// dup/loss. Native, so convergence no longer depends on the scale-test's external exporter.
    pub fn index_docs(label: &str, docs: u64) {
        gauge!("growlerdb_index_docs", "index" => label.to_string()).set(docs as f64);
    }

    /// Per-component index size in bytes: `component` is
    /// `term` (term dictionaries), `postings`, `positions` (phrase support), `fieldnorms` (BM25
    /// lengths) — together the classic inverted index — plus `fast` (fast-field cache), `store`
    /// (doc store), `locator` (hydration lookup), and `other` (metadata/deletes). Lets the
    /// index:source ratio be attributed to its drivers (a stacked panel) rather than a lump total,
    /// and lets storage work (dropping positions, fast-only numerics, compact key terms) be
    /// verified against the exact structure it targets. The components sum to
    /// `growlerdb_index_bytes` exactly.
    pub fn index_bytes_component(label: &str, component: &str, bytes: u64) {
        gauge!("growlerdb_index_bytes_component",
            "index" => label.to_string(), "component" => component.to_string())
        .set(bytes as f64);
    }

    /// Deleted-but-unpurged docs in a shard — the **delete debt** a size sample must be
    /// read against: under `NoMergePolicy` superseded/deleted docs stay on disk (and their keys in
    /// the term dictionaries) until a compaction merges them away, so `growlerdb_index_bytes`
    /// between merges overstates the steady-state footprint. Emitted on the compaction loop's tick
    /// alongside `segments_live`, from the same `CompactionHealth` read that drives the merge
    /// decision (the policy compacts at ≥20% debt).
    pub fn index_deleted_docs(label: &str, deleted: u64) {
        gauge!("growlerdb_index_deleted_docs", "index" => label.to_string()).set(deleted as f64);
    }

    /// Record a completed **compaction/merge**: bump `growlerdb_compactions_total`, add the
    /// segments reclaimed (`before - after`) to `growlerdb_segments_merged_total`, and update the
    /// live-segment gauge to the post-merge count. Labelled by `label` (index / windowed shard).
    /// A no-op merge (`before <= after`) still counts as a compaction attempt but reclaims nothing.
    pub fn compaction(label: &str, segments_before: u64, segments_after: u64) {
        counter!("growlerdb_compactions_total", "index" => label.to_string()).increment(1);
        let reclaimed = segments_before.saturating_sub(segments_after);
        counter!("growlerdb_segments_merged_total", "index" => label.to_string())
            .increment(reclaimed);
        segments_live(label, segments_after);
    }

    /// Record a **successful** run of a background maintenance loop: bumps a runs
    /// counter and sets a last-success-timestamp gauge (epoch seconds), both labelled by `loop_name`
    /// (e.g. `compaction`, `replica-refresh`, `jwks-refresh`, `registry-reload`, `cp-reload`,
    /// `pre-warm`). The gauge lets operators alert on staleness ("hasn't succeeded in N minutes")
    /// even when nothing is erroring loudly.
    pub fn background_success(loop_name: &str) {
        counter!("growlerdb_background_runs_total", "loop" => loop_name.to_string()).increment(1);
        gauge!("growlerdb_background_last_success_timestamp", "loop" => loop_name.to_string())
            .set(now_epoch_secs());
    }

    /// Record a **failed** run of a background maintenance loop: bumps a failures
    /// counter labelled by `loop_name`, so chronic background failure is alertable instead of only
    /// visible in stderr. Pairs with [`background_success`].
    pub fn background_failure(loop_name: &str) {
        counter!("growlerdb_background_failures_total", "loop" => loop_name.to_string())
            .increment(1);
    }

    /// Current time as epoch seconds (best-effort; `0` before 1970 / on clock error).
    fn now_epoch_secs() -> f64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as f64)
            .unwrap_or(0.0)
    }

    /// Record a completed query (search/aggregate): its `duration_secs` and whether it
    /// errored — drives QPS, error rate, and latency percentiles.
    pub fn query(duration_secs: f64, errored: bool) {
        counter!("growlerdb_query_total").increment(1);
        if errored {
            counter!("growlerdb_query_errors_total").increment(1);
        }
        histogram!("growlerdb_query_duration_seconds").record(duration_secs);
    }

    /// Record a completed **HTTP request** to the REST API: its matched `route`
    /// template (e.g. `/v1/search` — the router template, NOT the raw path, so label cardinality
    /// stays bounded), the response `status` code, and `duration_secs`. Drives the Runtime API
    /// panels (request rate, 4xx/5xx rate, p95 latency across every endpoint) and the Search
    /// "query status codes" panel (filter `route="/v1/search"`). A handful of status codes × a
    /// bounded set of route templates keeps the series count small.
    pub fn http_request(route: &str, status: u16, duration_secs: f64) {
        counter!("growlerdb_http_requests_total",
            "route" => route.to_string(), "status" => status.to_string())
        .increment(1);
        histogram!("growlerdb_http_request_duration_seconds", "route" => route.to_string())
            .record(duration_secs);
    }

    /// Record a built-in **login attempt** outcome: `outcome` is `success`,
    /// `bad_credential`, `locked` (throttled after repeated failures), or `busy` (shed under load).
    /// One counter `growlerdb_logins_total{outcome}` drives the Access panels — the sign-in rate and
    /// the failure rate (a brute-force / misconfiguration signal). OIDC logins are minted by the
    /// external IdP and never reach this handler, so this covers built-in credential logins only.
    pub fn login(outcome: &str) {
        counter!("growlerdb_logins_total", "outcome" => outcome.to_string()).increment(1);
    }

    /// Record indexed documents committed (ingestion throughput), labelled by `index` so the
    /// console can chart throughput per index (`sum(rate(...[5m])) by (index)`), not just
    /// cluster-wide.
    pub fn ingested_docs(index: &str, count: u64) {
        counter!("growlerdb_ingested_docs_total", "index" => index.to_string()).increment(count);
    }

    /// Record a completed **index write/commit**: the wall-clock `duration_secs` of the
    /// blocking stage+commit (Tantivy commit + location fsync + redb checkpoint), labelled by `index`.
    /// The latency counterpart to [`ingested_docs`](Self::ingested_docs) (throughput): when ingestion
    /// saturates, a rising write p95 *while node CPU stays flat* localizes the ceiling to the commit
    /// path rather than query or compaction (the exact gap that left the scale run unable to
    /// pinpoint its ~6.5k docs/s ceiling). Exports as a true histogram — the `_duration_seconds` suffix
    /// picks up the explicit latency buckets.
    pub fn write(index: &str, duration_secs: f64) {
        histogram!("growlerdb_write_duration_seconds", "index" => index.to_string())
            .record(duration_secs);
    }

    /// Record the node's current **write-queue depth**: in-flight + queued commits waiting
    /// on the write-admission semaphore. A gauge, labelled by `index` — backpressure shows here (depth
    /// climbing toward the admission limit) *before* it shows up as a growing `ingest_lag_ms`, so a
    /// connector out-running the commit path is visible directly, not just inferred.
    pub fn write_queue_depth(index: &str, depth: u64) {
        gauge!("growlerdb_write_queue_depth", "index" => index.to_string()).set(depth as f64);
    }

    /// Record the outcome of a **drift reconcile** cycle over a shard, labelled by
    /// `index` and shard `ordinal`. `stale` = indexed docs the source no longer holds (deleted),
    /// `missing` = source docs the shard owns but hadn't indexed (re-indexed). Both are counters so
    /// an alert can fire on any nonzero rate (`sum(rate(...[1h])) by (index) > 0` = the index silently
    /// drifted from the source and the backstop repaired it — investigate the ingest path). A
    /// `growlerdb_drift_reconcile_total` counter marks that a cycle ran, so "no drift" (a clean run)
    /// is distinguishable from "the job never ran".
    pub fn drift_reconcile(index: &str, ordinal: u32, stale: u64, missing: u64) {
        let ordinal = ordinal.to_string();
        counter!("growlerdb_drift_reconcile_total",
            "index" => index.to_string(), "ordinal" => ordinal.clone())
        .increment(1);
        counter!("growlerdb_drift_stale_total",
            "index" => index.to_string(), "ordinal" => ordinal.clone())
        .increment(stale);
        counter!("growlerdb_drift_missing_total",
            "index" => index.to_string(), "ordinal" => ordinal)
        .increment(missing);
    }

    /// Record a key-hydration (PK lookup): its `duration_secs`, how many locators Iceberg had
    /// rewritten (the **stale-locator / verify-fallback** signal), and how many of the `requested`
    /// keys were authoritatively `found` in the source.
    ///
    /// A rising **miss rate** (`requested - found`) is the cheap early-warning for index↔source
    /// **drift**: a stale index — e.g. after a recreated source — still returns search
    /// hits whose keys no longer exist in the table, so they fail to hydrate. It flags trouble even
    /// before the lineage guard engages on a restart.
    pub fn hydration(duration_secs: f64, refreshed_locators: u64, requested: u64, found: u64) {
        counter!("growlerdb_hydration_total").increment(1);
        histogram!("growlerdb_hydration_duration_seconds").record(duration_secs);
        counter!("growlerdb_hydration_keys_requested_total").increment(requested);
        counter!("growlerdb_hydration_keys_found_total").increment(found);
        if refreshed_locators > 0 {
            counter!("growlerdb_stale_locators_total").increment(refreshed_locators);
        }
    }

    /// Record the keys resolved by a hydration's locate (the layered
    /// path: key term → `_locid` fast field → `location.arr`). Counts **keys**, not
    /// requests, so locator traffic is directly rate-able.
    pub fn locate_keys(keys: u64) {
        counter!("growlerdb_locate_keys_total").increment(keys);
    }

    /// Record a completed **compaction re-map** event: an Iceberg
    /// rewrite removed interned data files from the live table, and the background re-map
    /// re-pointed the affected location slots at the rewritten rows' new files. Counts events
    /// and rows re-mapped, labelled by `index` — a rising `growlerdb_stale_locators_total`
    /// *despite* re-map events firing means the re-map isn't keeping up (or is skipping files,
    /// e.g. delete-bearing ones).
    pub fn locator_remap(index: &str, rows_remapped: u64) {
        counter!("growlerdb_locator_remap_events_total", "index" => index.to_string()).increment(1);
        counter!("growlerdb_locator_remapped_rows_total", "index" => index.to_string())
            .increment(rows_remapped);
    }

    /// Record the current **dead-file count**: interned data files flagged
    /// dead (rewritten away — permanent tombstones). A gauge labelled by `index`; it grows with
    /// each compaction the source table undergoes, and hydration skips point reads into these
    /// files, so the gauge doubles as a "compactions the index has absorbed" signal.
    pub fn locator_dead_files(index: &str, count: u64) {
        gauge!("growlerdb_locator_dead_files", "index" => index.to_string()).set(count as f64);
    }

    /// Record **source-health** gauges for an index's source table, sampled from Iceberg
    /// metadata GrowlerDB already reads (the current snapshot's `total-*` summary + the retained-
    /// snapshot count — no scan). These *diagnose* a source that wants Iceberg maintenance: GrowlerDB
    /// reads O(files) on the query path, so a source accumulating small files / long snapshot history
    /// silently slows queries. `avg_file_bytes` (= `bytes / data_files`) is the small-file signal — a
    /// falling average means many tiny files. The remedy (compaction / `expire_snapshots`) is the
    /// user's, outside GrowlerDB. Emitted per index on the control-plane ingestion sampler's tick.
    pub fn source_health(
        index: &str,
        data_files: u64,
        bytes: u64,
        delete_files: u64,
        records: u64,
        snapshots: u64,
    ) {
        let idx = || index.to_string();
        gauge!("growlerdb_source_data_files", "index" => idx()).set(data_files as f64);
        gauge!("growlerdb_source_bytes", "index" => idx()).set(bytes as f64);
        gauge!("growlerdb_source_delete_files", "index" => idx()).set(delete_files as f64);
        gauge!("growlerdb_source_records", "index" => idx()).set(records as f64);
        gauge!("growlerdb_source_snapshots", "index" => idx()).set(snapshots as f64);
        // The small-file signal as one alertable gauge: mean data-file size. 0 data files ⇒ 0.
        let avg = if data_files > 0 {
            bytes as f64 / data_files as f64
        } else {
            0.0
        };
        gauge!("growlerdb_source_avg_file_bytes", "index" => idx()).set(avg);
    }

    /// Record the source table's **partition skew**: the largest identity partition's
    /// record count over the mean (`1.0` = evenly sized; higher = a hotspot partition). Labelled by
    /// `index`; only emitted for cleanly identity-partitioned sources. Drives the Source "partition
    /// skew" panel — a lopsided-ingest / hot-key signal.
    pub fn source_partition_skew(index: &str, skew: f64) {
        gauge!("growlerdb_source_partition_skew", "index" => index.to_string()).set(skew);
    }

    /// Record **duplicate primary keys** a hydration key scan detected:
    /// extra distinct source rows matching an already-matched key — the source table is
    /// not unique on the composite key. The scan stays deterministic (highest
    /// `(file, position)` wins), but a nonzero rate means hydration/update/delete
    /// semantics are resolving an ambiguity the table shouldn't have — fix the source.
    /// Fires on the shared key-scan path: the `coordinates` strategy's verify-fallback
    /// **and** the `predicate` strategy's primary read.
    pub fn duplicate_pks(count: u64) {
        if count > 0 {
            counter!("growlerdb_duplicate_pks_total").increment(count);
        }
    }

    /// Record a hydration **plan-cache** outcome: whether pass 1's
    /// current-snapshot plan was reused from the snapshot-pinned cache (`hit`) or freshly
    /// planned — catalog + manifest reads (`miss`). The miss rate makes planning cost
    /// observable for the scale tests: a miss per batch means the cache isn't earning
    /// (snapshots advancing faster than lookups, or readers not long-lived).
    pub fn plan_cache(hit: bool) {
        if hit {
            counter!("growlerdb_plan_cache_hits_total").increment(1);
        } else {
            counter!("growlerdb_plan_cache_misses_total").increment(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    async fn get(router: &Router, path: &str) -> (StatusCode, String) {
        let resp = router
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn healthz_is_always_ok_and_readyz_flips_when_marked() {
        let readiness = Readiness::new();
        let router = health_router(readiness.clone());

        assert_eq!(get(&router, "/healthz").await.0, StatusCode::OK);
        // Not ready yet → 503.
        assert_eq!(
            get(&router, "/readyz").await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        readiness.mark_ready();
        assert_eq!(get(&router, "/readyz").await.0, StatusCode::OK);
    }

    #[tokio::test]
    async fn metrics_endpoint_renders_recorded_slis() {
        init("test"); // installs the recorder (idempotent)
        sli::query(0.012, false);
        sli::query(0.034, true);
        sli::ingested_docs("docs", 5);
        sli::hydration(0.002, 0, 10, 7); // 10 keys requested, 7 found → 3 hydration misses
        sli::ingest_lag_ms("docs", 45_000);
        sli::shard_availability("docs", 2, 3);
        sli::compaction("docs", 5, 1); // 4 segments reclaimed
        sli::segments_live("docs", 1);
        sli::duplicate_pks(2); // the key scan saw 2 extra rows for matched keys
        sli::source_health("docs", 1_000, 4_000_000, 3, 50_000, 42);
        sli::http_request("/v1/search", 200, 0.008);
        sli::login("success");
        sli::source_partition_skew("docs", 1.8);
        sli::index_docs("docs", 3); // convergence: index side

        let (status, body) = get(&health_router(Readiness::new()), "/metrics").await;
        assert_eq!(status, StatusCode::OK);
        // The recorded SLIs appear in the exposition (recorder may be installed by another
        // test first; assert the names are present rather than exact values).
        assert!(body.contains("growlerdb_query_total"));
        assert!(body.contains("growlerdb_query_errors_total"));
        assert!(body.contains("growlerdb_ingested_docs_total"));
        // latency metrics export as true histograms (`_bucket`), NOT summaries
        // (`{quantile}`), so `histogram_quantile` works and the console/Grafana p50/p95/p99 charts are
        // stable + aggregatable rather than decaying per-scrape estimates.
        assert!(
            body.contains("growlerdb_query_duration_seconds_bucket"),
            "query latency must export histogram buckets, not a summary"
        );
        assert!(body.contains("growlerdb_http_request_duration_seconds_bucket"));
        assert!(
            !body.contains("_duration_seconds{quantile"),
            "latency metrics must not export as summary quantiles"
        );
        // The hydration-miss drift SLI: keys requested vs found are exported.
        assert!(body.contains("growlerdb_hydration_keys_requested_total"));
        assert!(body.contains("growlerdb_hydration_keys_found_total"));
        // Ingestion-lag + shard-availability gauges, labelled by index.
        assert!(body.contains("growlerdb_ingest_lag_ms"));
        assert!(body.contains("growlerdb_shards_up"));
        assert!(body.contains("growlerdb_shards_total"));
        // Compaction / live-segments.
        assert!(body.contains("growlerdb_compactions_total"));
        assert!(body.contains("growlerdb_segments_merged_total"));
        assert!(body.contains("growlerdb_segments_live"));
        // Duplicate-PK detection on the hydration key scan.
        assert!(body.contains("growlerdb_duplicate_pks_total"));
        // REST RED metrics: per-route request counter + duration histogram.
        assert!(body.contains("growlerdb_http_requests_total"));
        assert!(body.contains("growlerdb_http_request_duration_seconds"));
        assert!(body.contains("growlerdb_logins_total"));
        assert!(body.contains("growlerdb_source_partition_skew"));
        // Native index doc count — the index side of source→index convergence.
        assert!(body.contains("growlerdb_index_docs"));
        // Source-health diagnostic gauges, labelled by index.
        assert!(body.contains("growlerdb_source_data_files"));
        assert!(body.contains("growlerdb_source_bytes"));
        assert!(body.contains("growlerdb_source_delete_files"));
        assert!(body.contains("growlerdb_source_records"));
        assert!(body.contains("growlerdb_source_snapshots"));
        // The small-file signal: mean data-file size = 4_000_000 / 1_000 = 4000 bytes.
        assert!(body.contains("growlerdb_source_avg_file_bytes"));
    }

    #[test]
    fn init_is_idempotent() {
        init("a");
        init("b"); // must not panic
    }

    #[test]
    fn shutdown_without_otlp_is_a_noop() {
        // No GROWLERDB_OTLP_ENDPOINT configured → no provider → shutdown is harmless.
        shutdown();
    }

    #[test]
    fn traces_endpoint_appends_the_signal_path_idempotently() {
        // The exporter uses with_endpoint verbatim (no path append), so we add /v1/traces.
        assert_eq!(
            traces_endpoint("http://lgtm:4318"),
            "http://lgtm:4318/v1/traces"
        );
        assert_eq!(
            traces_endpoint("http://lgtm:4318/"),
            "http://lgtm:4318/v1/traces"
        );
        // Already-full paths aren't doubled.
        assert_eq!(
            traces_endpoint("http://lgtm:4318/v1/traces"),
            "http://lgtm:4318/v1/traces"
        );
    }
}
