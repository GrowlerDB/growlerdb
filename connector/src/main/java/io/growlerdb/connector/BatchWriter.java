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
   * The end-of-trigger / drain <b>barrier</b> (task-194): whether <b>every</b> shard has durably
   * committed exactly {@code head}. Distinct from {@link #checkpointSnapshotId} (the MIN, for
   * resume) — this requires <b>all</b> shards to have converged on the head, so a drain gate can
   * assert convergence (per-shard {@code GetCheckpoint == lineage head}) instead of sleeping a fixed
   * interval and hoping ingest caught up. A shard still behind — or with no checkpoint yet — makes
   * it {@code false}. Because empty windows now advance every shard in lockstep, a fully-drained
   * sharded cluster converges here exactly.
   */
  boolean drainedTo(long head);

  @Override
  void close() throws InterruptedException;
}
