package io.growlerdb.connector;

/**
 * Thrown by {@link ConnectorJob#runOnce} when a trigger window's changelog carries <b>fewer rows
 * than its source snapshots committed</b> — an <b>under-read</b>. This guard closes a silent row loss
 * under a compaction race: an empty/short window would otherwise jump the cursor to head and a later
 * batch would stamp a checkpoint over the gap, making the loss permanent and evidence-erasing.
 *
 * <p>Expected is {@code Σ summary['added-records']} over the window's {@code append} snapshots;
 * observed is the changelog rows the scan returned. For a healthy append window these are equal
 * (every appended record surfaces as exactly one INSERT row), so a shortfall means the scan dropped
 * rows.
 *
 * <p>Throwing does <b>not</b> advance the cursor: the trigger fails, the streaming query restarts,
 * and the connector re-reads the window from the Node's durable checkpoint — a transient scan race
 * self-heals, a persistent mismatch stays a loud stall. The gate applies only to append (and
 * layout-only {@code replace}) windows where the count is exact; {@code overwrite}/{@code delete}
 * windows are exempt (the reconcile job is the backstop there).
 */
public final class IngestUnderReadException extends RuntimeException {

  public IngestUnderReadException(String table, long fromExclusive, long head, long observed, long expected) {
    super(
        "INGEST_UNDER_READ: changelog for `"
            + table
            + "` window ("
            + fromExclusive
            + " -> "
            + head
            + "] returned "
            + observed
            + " row(s) but its append snapshots committed "
            + expected
            + " record(s). Refusing to advance the checkpoint over the "
            + (expected - observed)
            + "-row gap — the changelog scan under-read (a changelog/compaction race). Ingest halts"
            + " loudly; it will re-read this window on restart.");
  }
}
