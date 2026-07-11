package io.growlerdb.connector;

/**
 * Thrown by {@link ConnectorJob#runOnce} when the changelog it read for a trigger window carries
 * <b>fewer rows than the source snapshots in that window committed</b> — an <b>under-read</b>: the
 * changelog scan missed rows a snapshot physically appended. This guard closes a silent row loss
 * under a compaction race: an empty/short window would otherwise jump the in-memory cursor to head
 * and a later batch would stamp a later checkpoint over the gap, making the loss permanent and
 * evidence-erasing.
 *
 * <p>The expected count is {@code Σ summary['added-records']} over the window's {@code append}
 * snapshots — the source of truth for how many records physically landed — and the observed count is
 * the changelog rows the scan returned. In a healthy scan these are equal for an append window
 * (every appended record surfaces as exactly one INSERT row; the changelog counts physical rows, so
 * even a duplicate primary key — which GrowlerDB later collapses last-write-wins in the engine —
 * appears once per physical append). A shortfall therefore means the scan genuinely dropped rows.
 *
 * <p>Throwing here means the cursor does <b>not</b> advance: the trigger fails, the streaming query
 * restarts, and the connector re-reads the same window from the Node's durable checkpoint. A
 * transient scan race self-heals on the re-read; a persistent mismatch stays a loud, visible stall
 * instead of permanent silent loss. The gate applies only to windows composed of {@code append}
 * (and layout-only {@code replace}/compaction) snapshots — the changelog scan skips {@code replace}
 * snapshots and every append row is an INSERT, so the count is exact. Windows containing
 * {@code overwrite}/{@code delete} snapshots (row-level updates/deletes, where the changelog's net
 * diff legitimately diverges from physical {@code added-records}) are exempt; the systematic backstop
 * there is the reconcile job.
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
