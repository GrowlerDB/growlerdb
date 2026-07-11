package io.growlerdb.connector;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import java.io.IOException;

/**
 * The connector's own Prometheus metrics: per-trigger rows read vs. expected, per-shard acks, stream
 * restarts, write retries, under-read stalls. Counters/gauges scraped off a tiny in-process HTTP
 * server, so the ingest-side signals survive log rotation (Spark INFO floods it) and drive alerts
 * (e.g. {@code rate(under_reads_total) > 0}).
 *
 * <p>Static holder to fit the connector's static/lambda call sites without threading an object
 * through every seam. Metric updates are always cheap and safe to call; the HTTP endpoint is started
 * only when {@link #startServer} is invoked (the entrypoint does so when {@code GROWLERDB_METRICS_PORT}
 * is set), so unit tests and local one-shot runs never bind a port.
 */
public final class ConnectorMetrics {

  /** Env var naming the port the metrics HTTP server binds; unset ⇒ no server (metrics still count). */
  static final String PORT_ENV = "GROWLERDB_METRICS_PORT";

  static final Counter TRIGGERS =
      Counter.build()
          .name("growlerdb_connector_triggers_total")
          .help("Micro-batch triggers that committed at least one op.")
          .register();

  static final Counter ROWS_READ =
      Counter.build()
          .name("growlerdb_connector_rows_read_total")
          .help("Changelog rows read across all trigger windows.")
          .register();

  static final Counter ROWS_EXPECTED =
      Counter.build()
          .name("growlerdb_connector_rows_expected_total")
          .help("Expected rows (Σ added-records over append snapshots) when the under-read gate applied.")
          .register();

  static final Counter UNDER_READS =
      Counter.build()
          .name("growlerdb_connector_under_reads_total")
          .help("Trigger windows the expected-row-count gate refused as an under-read.")
          .register();

  static final Counter SHARD_ACKS =
      Counter.build()
          .name("growlerdb_connector_shard_acks_total")
          .help("Per-shard sub-batch acknowledgements (a committed write to a shard).")
          .labelNames("shard")
          .register();

  static final Counter STREAM_RESTARTS =
      Counter.build()
          .name("growlerdb_connector_stream_restarts_total")
          .help("In-process streaming-query restarts after a micro-batch failure.")
          .register();

  static final Counter WRITE_RETRIES =
      Counter.build()
          .name("growlerdb_connector_write_retries_total")
          .help("Write-client RPC retries over transient failures, by gRPC status code.")
          .labelNames("code")
          .register();

  static final Gauge CHECKPOINT =
      Gauge.build()
          .name("growlerdb_connector_checkpoint_snapshot")
          .help("The Iceberg snapshot id the connector last advanced the cursor to.")
          .register();

  static final Gauge LAST_TRIGGER_ROWS =
      Gauge.build()
          .name("growlerdb_connector_last_trigger_rows")
          .help("Changelog rows in the most recent trigger window.")
          .register();

  private static volatile HTTPServer server;

  private ConnectorMetrics() {}

  /**
   * Start the metrics HTTP server on {@code GROWLERDB_METRICS_PORT} if it is set (a no-op otherwise,
   * so local one-shot runs don't bind a port). Idempotent; a bind failure is logged and swallowed —
   * losing metrics must never take down ingestion.
   */
  static void startServer() {
    String port = System.getenv(PORT_ENV);
    if (port == null || port.isBlank() || server != null) {
      return;
    }
    try {
      server = new HTTPServer.Builder().withPort(Integer.parseInt(port.trim())).build();
      System.out.printf("connector: metrics on :%s/metrics%n", port.trim());
    } catch (NumberFormatException | IOException e) {
      System.err.printf("connector: metrics server failed to start on `%s`: %s%n", port, e);
    }
  }

  /** Record a committed trigger window: rows read, expected (−1 when the gate didn't apply), head. */
  static void recordTrigger(long rowsRead, long expected, long checkpointSnapshot) {
    TRIGGERS.inc();
    ROWS_READ.inc(rowsRead);
    if (expected >= 0) {
      ROWS_EXPECTED.inc(expected);
    }
    LAST_TRIGGER_ROWS.set(rowsRead);
    CHECKPOINT.set(checkpointSnapshot);
  }

  /** Record that the expected-row-count gate refused a window as an under-read (no cursor advance). */
  static void recordUnderRead() {
    UNDER_READS.inc();
  }

  /** Record a committed sub-batch ack to shard {@code ordinal}. */
  static void recordShardAck(int ordinal) {
    SHARD_ACKS.labels(Integer.toString(ordinal)).inc();
  }

  /** Record an in-process streaming-query restart. */
  static void recordStreamRestart() {
    STREAM_RESTARTS.inc();
  }

  /** Record a write-client retry over a transient gRPC {@code code}. */
  static void recordWriteRetry(String code) {
    WRITE_RETRIES.labels(code).inc();
  }
}
