package io.growlerdb.connector;

import io.growlerdb.proto.v1.DocBatch;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.concurrent.Callable;

/**
 * The write client of one worker in a <b>parallel connector set</b>: the worker owns
 * a fixed {@link ShardGroup shard group} and this client sends a batch's sub-batches — including
 * empty lockstep advances — to its <b>owned shards only</b>, dropping the rest (their owners send
 * them). Every shard keeps exactly one writer, so the Node's per-shard continuity guard holds
 * with no writer identity, and a worker's checkpoint namespace is structurally its group:
 *
 * <ul>
 *   <li>{@link #checkpointSnapshotId()} — the lineage-min over the OWNED shards (the worker can
 *       only resume what it drives);
 *   <li>{@link #drainedTo(long)} — convergence of the OWNED shards;
 *   <li>the {@code safe_checkpoint} prune floor stays the trigger's own resume point: with the
 *       window-covering guard a replay no-ops by <i>position</i>, so pruning can never manufacture
 *       a gap even across worker-count changes (regrouping needs no coordination at all — the new
 *       owner just resumes from its new group's lineage-min).
 * </ul>
 *
 * <p>Partitioning reuses {@link ShardedWriteClient#partition} verbatim, so per-shard batch ids
 * ({@code {id}#s{ordinal}}) and op placement are identical to the single-connector path — the two
 * modes are interchangeable on the same table (stop one, start the other; resume comes from the
 * Nodes' checkpoints).
 */
public final class ShardGroupWriteClient implements BatchWriter {

  private final SortedSet<Integer> owned;
  /** Owned ordinal → its Node client. Only owned shards get channels. */
  private final Map<Integer, WriteClient> byOrdinal = new TreeMap<>();
  private final ShardRouter router;
  private final ShardFanOut fanOut;
  private final SnapshotLineage lineage;

  /**
   * @param endpoints ALL shard endpoints in ordinal order (the full topology — the group is a
   *     subset of it); must match the router's shard count
   * @param owned the shard ordinals this worker owns ({@link ShardGroup#owned})
   */
  public ShardGroupWriteClient(
      List<String> endpoints, ShardRouter router, SnapshotLineage lineage, SortedSet<Integer> owned) {
    if (owned.isEmpty()) {
      throw new IllegalArgumentException("a shard-group writer needs at least one owned shard");
    }
    if (router.shards() != endpoints.size()) {
      throw new IllegalArgumentException(
          "router covers " + router.shards() + " shards but got " + endpoints.size() + " endpoints");
    }
    if (owned.first() < 0 || owned.last() >= endpoints.size()) {
      throw new IllegalArgumentException(
          "owned shards " + owned + " out of range for " + endpoints.size() + " endpoints");
    }
    this.owned = owned;
    this.router = router;
    this.lineage = lineage;
    this.fanOut = new ShardFanOut(owned.size());
    try {
      for (int ordinal : owned) {
        String endpoint = endpoints.get(ordinal);
        String[] hp = endpoint.split(":", 2);
        if (hp.length != 2) {
          throw new IllegalArgumentException("endpoint must be host:port, got `" + endpoint + "`");
        }
        byOrdinal.put(ordinal, new WriteClient(hp[0].trim(), Integer.parseInt(hp[1].trim())));
      }
    } catch (RuntimeException e) {
      // Don't leak channels of clients already opened when a later endpoint is malformed.
      closeQuietly();
      throw e;
    }
  }

  @Override
  public long write(DocBatch batch) {
    // Partition over ALL ordinals — identical ids/placement to the single-connector path —
    // then send only the owned sub-batches (empties included for lockstep within the group).
    List<DocBatch> perShard = ShardedWriteClient.partition(batch, router);
    List<Callable<Long>> writes = new ArrayList<>(owned.size());
    for (int ordinal : owned) {
      final int shard = ordinal;
      final DocBatch sub = perShard.get(shard);
      writes.add(
          () -> {
            long snapshot = byOrdinal.get(shard).write(sub);
            ConnectorMetrics.recordShardAck(shard);
            return snapshot;
          });
    }
    return fanOut.maxSnapshot(writes);
  }

  /** The lineage-min resume point over the OWNED shards (null: an owned shard is empty). */
  @Override
  public Long checkpointSnapshotId() {
    return ShardedWriteClient.resumeMin(new ArrayList<>(byOrdinal.values()), lineage);
  }

  /** Drained when every OWNED shard has converged on {@code head} — a worker drives only its own. */
  @Override
  public boolean drainedTo(long head) {
    for (WriteClient shard : byOrdinal.values()) {
      if (!shard.drainedTo(head)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void close() throws InterruptedException {
    InterruptedException first = null;
    try {
      fanOut.close();
    } catch (InterruptedException e) {
      first = e;
    }
    for (WriteClient shard : byOrdinal.values()) {
      try {
        shard.close();
      } catch (InterruptedException e) {
        if (first == null) {
          first = e;
        }
      }
    }
    if (first != null) {
      throw first;
    }
  }

  private void closeQuietly() {
    try {
      close();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
