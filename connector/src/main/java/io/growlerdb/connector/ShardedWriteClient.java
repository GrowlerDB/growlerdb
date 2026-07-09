package io.growlerdb.connector;

import io.growlerdb.proto.v1.Coordinates;
import io.growlerdb.proto.v1.DocBatch;
import io.growlerdb.proto.v1.DocOp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Fans a {@link DocBatch} out across a sharded cluster: each op is routed by its key to the
 * shard that owns it ({@link ShardRouter}), and the per-shard sub-batch is committed to that
 * shard's Node. This is the write-side counterpart of the Gateway's read routing — a document
 * is written to the same shard a later key lookup will query.
 *
 * <p>Mirrors {@code ShardRouter::partition_batch} in {@code growlerdb-core}: op order is
 * preserved within a shard, every sub-batch carries the same checkpoint, and each gets a
 * per-shard {@code batch_id} ({@code {id}#s{ordinal}}) so an idempotent replay stays
 * shard-unique. Empty sub-batches are sent too — they advance their shard's checkpoint in
 * lockstep (task-194); the resume checkpoint is the position <i>every</i> shard has durably
 * passed. Sub-batches commit <b>concurrently</b> ({@link ShardFanOut}, task-196), joined
 * per batch so per-shard order across batches is preserved.
 */
public final class ShardedWriteClient implements BatchWriter {

  private final List<WriteClient> shards;
  private final ShardRouter router;
  private final ShardFanOut fanOut;
  /** Fills in sequence numbers the Nodes don't have (legacy stored checkpoints) at resume. */
  private final SnapshotLineage lineage;

  /** Connect a Node per {@code host:port} endpoint, routed by {@code strategy} (legacy). */
  public ShardedWriteClient(List<String> endpoints, ShardRouter.Strategy strategy) {
    this(endpoints, new ShardRouter(endpoints.size(), strategy));
  }

  /** As below, with no table lineage (resume falls back to legacy behavior on missing data). */
  public ShardedWriteClient(List<String> endpoints, ShardRouter router) {
    this(endpoints, router, SnapshotLineage.none());
  }

  /**
   * Connect a Node per {@code host:port} endpoint, placing writes with an explicit {@code router}
   * (task-77: a bucketed router built from the registry's vended map, so write placement matches
   * the Gateway's read routing). The router's shard count must equal the endpoint count.
   */
  public ShardedWriteClient(List<String> endpoints, ShardRouter router, SnapshotLineage lineage) {
    if (endpoints.isEmpty()) {
      throw new IllegalArgumentException("a ShardedWriteClient needs at least one endpoint");
    }
    if (router.shards() != endpoints.size()) {
      throw new IllegalArgumentException(
          "router covers " + router.shards() + " shards but got " + endpoints.size() + " endpoints");
    }
    // Validate + parse EVERY endpoint before opening any channel (task-152 / B16): a `new WriteClient`
    // eagerly opens a gRPC channel, so a malformed later endpoint throwing mid-loop would leak the
    // channels of the ones already created. Parsing first means we only start connecting once all
    // endpoints are known-good.
    record HostPort(String host, int port) {}
    List<HostPort> parsed = new ArrayList<>(endpoints.size());
    for (String endpoint : endpoints) {
      String[] hp = endpoint.split(":", 2);
      if (hp.length != 2) {
        throw new IllegalArgumentException("endpoint must be host:port, got `" + endpoint + "`");
      }
      int port;
      try {
        port = Integer.parseInt(hp[1].trim());
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("endpoint port is not a number: `" + endpoint + "`");
      }
      parsed.add(new HostPort(hp[0].trim(), port));
    }
    List<WriteClient> clients = new ArrayList<>(parsed.size());
    for (HostPort hp : parsed) {
      clients.add(new WriteClient(hp.host(), hp.port()));
    }
    this.shards = List.copyOf(clients);
    this.router = router;
    this.fanOut = new ShardFanOut(this.shards.size());
    this.lineage = lineage;
  }

  @Override
  public long write(DocBatch batch) {
    List<DocBatch> perShard = partition(batch, router);
    List<Callable<Long>> writes = new ArrayList<>(perShard.size());
    for (int ordinal = 0; ordinal < perShard.size(); ordinal++) {
      final int shard = ordinal;
      final DocBatch sub = perShard.get(shard);
      // Send EVERY sub-batch, including empties (task-194). A window that routed no rows to a shard
      // still advances that shard's checkpoint (a redb-only no-op commit on the Node), keeping all
      // shards in lockstep at the trigger head. Skipping empties let shards drift — which inflated
      // the min-checkpoint resume re-read AND breaks the Node's continuity guard, whose `from ==
      // current` invariant only holds when every shard tracks the same source position.
      writes.add(
          () -> {
            long snapshot = shards.get(shard).write(sub);
            // Per-shard ack (task-194 AC6): the metric that makes the "1-of-3-landed" loss
            // signature visible — a shard whose acks stall relative to its siblings is the tell.
            // Recorded on success only, inside the task.
            ConnectorMetrics.recordShardAck(shard);
            return snapshot;
          });
    }
    // Concurrent fan-out with a join-all barrier (task-196): the slowest shard bounds the batch,
    // and no next batch starts until every shard settled this one (per-shard order preserved).
    return fanOut.maxSnapshot(writes);
  }

  /**
   * Split {@code batch} into one sub-batch per shard ordinal {@code [0, router.shards())},
   * routing each op by its key and preserving op order within a shard. Mirrors Rust
   * {@code ShardRouter::partition_batch}: same checkpoint on every sub-batch, per-shard
   * {@code batch_id} ({@code {id}#s{ordinal}}). Pure (no I/O), so the placement is unit-tested
   * without a live cluster.
   */
  static List<DocBatch> partition(DocBatch batch, ShardRouter router) {
    int n = router.shards();
    List<List<DocOp>> perShard = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      perShard.add(new ArrayList<>());
    }
    for (DocOp op : batch.getOpsList()) {
      perShard.get(router.route(keyOf(op))).add(op);
    }
    List<DocBatch> out = new ArrayList<>(n);
    for (int ordinal = 0; ordinal < n; ordinal++) {
      DocBatch.Builder sub =
          DocBatch.newBuilder()
              .addAllOps(perShard.get(ordinal))
              .setCheckpoint(batch.getCheckpoint())
              .setBatchId(batch.getBatchId() + "#s" + ordinal);
      // Carry `from` onto each sub-batch so every shard's continuity guard (task-194) sees the
      // window's resume point; all shards resume from the same source position.
      if (batch.hasFromCheckpoint()) {
        sub.setFromCheckpoint(batch.getFromCheckpoint());
      }
      // Carry the resume FLOOR too so each shard prunes idempotency records it can never be re-sent
      // (task-204). It's the same across shards — the min committed checkpoint the connector resumes
      // the whole cluster from — so each shard's local prune stays sound.
      if (batch.hasSafeCheckpoint()) {
        sub.setSafeCheckpoint(batch.getSafeCheckpoint());
      }
      out.add(sub.build());
    }
    return out;
  }

  /**
   * The resume checkpoint is the <b>minimum</b> committed snapshot across shards — in
   * <b>lineage order</b> (Iceberg sequence numbers, task-196): snapshot ids are random longs,
   * so the old numeric {@code Math.min} picked the wrong shard ~half the time two shards
   * diverged (task-205). Replaying from the lineage-min re-applies nothing new on shards
   * already ahead (the Node's window-covering guard no-ops them by position) and misses
   * nothing on the shard that lagged. {@code null} on any shard ⇒ start from the beginning
   * (that shard has committed nothing yet).
   *
   * <p>Sequence numbers come from the Nodes' stored checkpoints, backfilled from the table's
   * own metadata ({@link SnapshotLineage}) for legacy stored values. Only when a divergent
   * snapshot is unknown everywhere (expired + never stamped) does this degrade to the old
   * numeric min — kept as the pre-existing fallback, now with a loud warning.
   */
  @Override
  public Long checkpointSnapshotId() {
    return resumeMin(shards, lineage);
  }

  /**
   * The lineage-min resume point over {@code clients} (see {@link #checkpointSnapshotId()}), or
   * {@code null} when any shard has no checkpoint. Shared with the shard-group client
   * (task-196), whose resume is the same computation over its owned subset.
   */
  static Long resumeMin(List<WriteClient> clients, SnapshotLineage lineage) {
    List<WriteClient.ShardCheckpoint> checkpoints = new ArrayList<>(clients.size());
    for (WriteClient shard : clients) {
      WriteClient.ShardCheckpoint cp = shard.checkpoint();
      if (cp == null) {
        return null;
      }
      checkpoints.add(cp);
    }
    return resumeMinOf(checkpoints, lineage);
  }

  /**
   * The lineage-min resume point over already-fetched {@code checkpoints} (see
   * {@link #checkpointSnapshotId()}). Shared with the windowed client (task-219), which fetches a
   * checkpoint per <i>window</i> rather than per ordinal shard. {@code checkpoints} must be non-empty.
   */
  static Long resumeMinOf(List<WriteClient.ShardCheckpoint> checkpoints, SnapshotLineage lineage) {
    // Converged shards need no order at all — the common steady-state (lockstep advance).
    long first = checkpoints.get(0).snapshotId();
    if (checkpoints.stream().allMatch(cp -> cp.snapshotId() == first)) {
      return first;
    }
    Long minId = null;
    long minSeq = Long.MAX_VALUE;
    for (WriteClient.ShardCheckpoint cp : checkpoints) {
      java.util.OptionalLong seq =
          cp.sequenceNumber().isPresent()
              ? cp.sequenceNumber()
              : lineage.sequenceOf(cp.snapshotId());
      if (seq.isEmpty()) {
        long numericMin =
            checkpoints.stream()
                .mapToLong(WriteClient.ShardCheckpoint::snapshotId)
                .min()
                .orElseThrow();
        System.err.printf(
            "ShardedWriteClient: shard checkpoints diverge and snapshot %d has no known sequence "
                + "number — falling back to the NUMERIC min %d, which is not lineage order "
                + "(task-205); resume may pick the wrong shard%n",
            cp.snapshotId(), numericMin);
        return numericMin;
      }
      if (seq.getAsLong() < minSeq) {
        minSeq = seq.getAsLong();
        minId = cp.snapshotId();
      }
    }
    return minId;
  }

  /** Drained only when <b>every</b> shard's checkpoint has converged on {@code head} (task-194). */
  @Override
  public boolean drainedTo(long head) {
    for (WriteClient shard : shards) {
      if (!shard.drainedTo(head)) {
        return false;
      }
    }
    return true;
  }

  private static Coordinates keyOf(DocOp op) {
    return switch (op.getOpCase()) {
      case UPSERT -> op.getUpsert().getDoc().getKey();
      case DELETE -> op.getDelete();
      case OP_NOT_SET -> throw new IllegalArgumentException("DocOp has no op set");
    };
  }

  @Override
  public void close() throws InterruptedException {
    InterruptedException first = null;
    try {
      // Drain the fan-out pool before tearing channels down so no in-flight write loses its
      // channel mid-RPC.
      fanOut.close();
    } catch (InterruptedException e) {
      first = e;
    }
    for (WriteClient shard : shards) {
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
}
