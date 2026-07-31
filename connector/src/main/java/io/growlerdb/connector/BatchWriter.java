package io.growlerdb.connector;

import io.growlerdb.proto.v1.DocBatch;

/**
 * The connector's view of "somewhere to commit a {@link DocBatch}": either a single Node
 * ({@link WriteClient}) or a sharded cluster ({@link ShardedWriteClient}) that fans the batch
 * out to the owning shard of each op. {@link ConnectorJob} writes through this seam, so it is
 * identical whether the target is one shard or many.
 */
public interface BatchWriter extends AutoCloseable {

  /** Commit a batch; returns a representative committed index snapshot. */
  long write(DocBatch batch);

  /**
   * The source checkpoint to <b>resume</b> from after a restart, or {@code null} to start from
   * the beginning of the changelog. For a sharded target this is the position every shard has
   * durably passed (so a replay re-applies nothing new; {@code batch_id} dedups the boundary).
   */
  Long checkpointSnapshotId();

  /**
   * The drain <b>barrier</b>: whether <b>every</b> shard has durably committed exactly {@code head}.
   * Distinct from {@link #checkpointSnapshotId} (the MIN, for resume) — the barrier lets a drain gate
   * assert convergence instead of sleeping and hoping ingest caught up. A shard still behind — or with
   * no checkpoint yet — makes it {@code false}.
   */
  boolean drainedTo(long head);

  @Override
  void close() throws InterruptedException;
}
