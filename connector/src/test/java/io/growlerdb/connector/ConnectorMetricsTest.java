package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * The connector metrics count the ingest-side signals. Counters are process-global (Prometheus
 * default registry), so assert on deltas rather than
 * absolute values — other tests in the JVM may have touched them.
 */
class ConnectorMetricsTest {

  @Test
  void triggerRecordsRowsExpectedAndCheckpoint() {
    double reads0 = ConnectorMetrics.ROWS_READ.get();
    double expected0 = ConnectorMetrics.ROWS_EXPECTED.get();
    double triggers0 = ConnectorMetrics.TRIGGERS.get();

    ConnectorMetrics.recordTrigger(7, 7, 42);
    assertEquals(triggers0 + 1, ConnectorMetrics.TRIGGERS.get());
    assertEquals(reads0 + 7, ConnectorMetrics.ROWS_READ.get());
    assertEquals(expected0 + 7, ConnectorMetrics.ROWS_EXPECTED.get());
    assertEquals(7, ConnectorMetrics.LAST_TRIGGER_ROWS.get());
    assertEquals(42, ConnectorMetrics.CHECKPOINT.get());

    // An exempt window (expected < 0) counts the read rows but not the expected counter.
    double expected1 = ConnectorMetrics.ROWS_EXPECTED.get();
    ConnectorMetrics.recordTrigger(3, -1, 43);
    assertEquals(expected1, ConnectorMetrics.ROWS_EXPECTED.get(), "no expected count for an exempt window");
    assertEquals(43, ConnectorMetrics.CHECKPOINT.get());
  }

  @Test
  void underReadStreamRestartAndPerShardAcksCount() {
    double underReads0 = ConnectorMetrics.UNDER_READS.get();
    double restarts0 = ConnectorMetrics.STREAM_RESTARTS.get();
    double ack0 = ConnectorMetrics.SHARD_ACKS.labels("0").get();
    double retry0 = ConnectorMetrics.WRITE_RETRIES.labels("UNAVAILABLE").get();

    ConnectorMetrics.recordUnderRead();
    ConnectorMetrics.recordStreamRestart();
    ConnectorMetrics.recordShardAck(0);
    ConnectorMetrics.recordWriteRetry("UNAVAILABLE");

    assertEquals(underReads0 + 1, ConnectorMetrics.UNDER_READS.get());
    assertEquals(restarts0 + 1, ConnectorMetrics.STREAM_RESTARTS.get());
    assertEquals(ack0 + 1, ConnectorMetrics.SHARD_ACKS.labels("0").get());
    assertEquals(retry0 + 1, ConnectorMetrics.WRITE_RETRIES.labels("UNAVAILABLE").get());
  }
}
