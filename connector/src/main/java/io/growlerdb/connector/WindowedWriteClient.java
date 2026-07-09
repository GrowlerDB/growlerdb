package io.growlerdb.connector;

import io.growlerdb.proto.v1.DocBatch;
import io.growlerdb.proto.v1.DocOp;
import io.growlerdb.proto.v1.Value;
import io.growlerdb.proto.v1.WindowingConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Streams a {@link DocBatch} into a <b>distributed windowed</b> index (task-219): each upsert is
 * routed to its <b>time window</b> ({@link WindowRouter}, byte-identical to the engine), the window's
 * owning node is resolved from the control plane (placed on first ask), and the window's sub-batch is
 * committed to that node — the write-side of CP-driven windowed placement, mirroring the engine's
 * batch {@code write_windowed}.
 *
 * <p>Unlike {@link ShardedWriteClient} (fixed ordinal shards, lockstep empty-batches), windows form
 * continuously and each advances independently: sub-batches carry <b>no</b> {@code from}/{@code safe}
 * checkpoint (matching {@code TimeWindowing::partition_batch}, which uses {@code from = None}), so a
 * window that skipped a batch doesn't trip the node's continuity guard. The resume point is the min
 * committed checkpoint across the windows that have committed (idempotent replay; {@code batch_id}
 * dedups). Deletes carry no window value, so they broadcast to every touched/known window (the owner
 * re-broadcasts to its own windows); append-mostly sources rarely delete.
 */
public final class WindowedWriteClient implements BatchWriter {

  private final String index;
  private final ControlPlaneClient controlPlane;
  private final WindowRouter windowRouter;
  private final SnapshotLineage lineage;
  /** window → the write client for its owning node (resolved from the CP, cached). */
  private final ConcurrentMap<Long, WriteClient> windowClient = new ConcurrentHashMap<>();
  /** node endpoint → write client (one channel per node, shared across its windows). */
  private final ConcurrentMap<String, WriteClient> byEndpoint = new ConcurrentHashMap<>();

  public WindowedWriteClient(
      String index, ControlPlaneClient controlPlane, WindowingConfig windowing, SnapshotLineage lineage) {
    this.index = index;
    this.controlPlane = controlPlane;
    this.windowRouter = new WindowRouter(windowing);
    this.lineage = lineage;
  }

  @Override
  public long write(DocBatch batch) {
    long maxSnapshot = 0L;
    for (var entry : partition(batch, windowRouter, windowClient.keySet()).entrySet()) {
      maxSnapshot = Math.max(maxSnapshot, clientForWindow(entry.getKey()).write(entry.getValue()));
    }
    return maxSnapshot;
  }

  /**
   * Split {@code batch} into one sub-batch per time window, routing each upsert by its window field
   * ({@link WindowRouter}) and broadcasting deletes to every touched window plus every {@code
   * knownWindow} (a delete carries no window value; the owner re-broadcasts to its own windows).
   * Each sub-batch carries the same checkpoint and a per-window {@code batch_id} ({@code {id}#w{window}}),
   * and — matching {@code TimeWindowing::partition_batch} — <b>no</b> {@code from}/{@code safe}
   * checkpoint, so a window that skipped a batch isn't gap-rejected by the node's continuity guard.
   * Pure (no I/O), so the placement is unit-tested without a live cluster (task-219).
   */
  static SortedMap<Long, DocBatch> partition(
      DocBatch batch, WindowRouter router, java.util.Set<Long> knownWindows) {
    SortedMap<Long, List<DocOp>> byWindow = new TreeMap<>();
    List<DocOp> deletes = new ArrayList<>();
    for (DocOp op : batch.getOpsList()) {
      switch (op.getOpCase()) {
        case UPSERT -> {
          Value wv = op.getUpsert().getDoc().getFieldsMap().get(router.field());
          if (wv == null) {
            throw new IllegalStateException(
                "upsert is missing the window field `"
                    + router.field()
                    + "` — add it to --fields so the connector can route by window (task-219)");
          }
          byWindow.computeIfAbsent(router.windowOf(wv), w -> new ArrayList<>()).add(op);
        }
        case DELETE -> deletes.add(op);
        case OP_NOT_SET -> throw new IllegalArgumentException("DocOp has no op set");
      }
    }
    SortedSet<Long> targets = new TreeSet<>(byWindow.keySet());
    if (!deletes.isEmpty()) {
      targets.addAll(knownWindows);
    }
    SortedMap<Long, DocBatch> out = new TreeMap<>();
    for (long window : targets) {
      List<DocOp> ops = new ArrayList<>(byWindow.getOrDefault(window, List.of()));
      ops.addAll(deletes);
      out.put(
          window,
          DocBatch.newBuilder()
              .addAllOps(ops)
              .setCheckpoint(batch.getCheckpoint())
              .setBatchId(batch.getBatchId() + "#w" + window)
              .build());
    }
    return out;
  }

  /** The write client for a window's owning node — resolved from the CP (placed on first ask), cached. */
  private WriteClient clientForWindow(long window) {
    return windowClient.computeIfAbsent(
        window,
        w -> {
          String endpoint = controlPlane.resolveWindowOwner(index, w).getEndpoint();
          return byEndpoint.computeIfAbsent(endpoint, WindowedWriteClient::connect);
        });
  }

  @Override
  public Long checkpointSnapshotId() {
    // Resume = the min committed checkpoint across the windows that have committed, in lineage order.
    // A just-placed but un-written window has no checkpoint and doesn't constrain resume. If no window
    // has committed yet, start from the beginning. Correct (idempotent replay); bounding this to the
    // active windows (so a cold restart doesn't re-read from the oldest window) is a follow-up.
    List<WriteClient.ShardCheckpoint> committed = windowCheckpoints();
    if (committed.isEmpty()) {
      return null;
    }
    return ShardedWriteClient.resumeMinOf(committed, lineage);
  }

  @Override
  public boolean drainedTo(long head) {
    // Old windows correctly lag (they stop receiving rows), so "every window at head" never holds for
    // a windowed index. The connector has pushed through `head` when the frontier (most-advanced)
    // window has reached it — the current window catches up last.
    List<WriteClient.ShardCheckpoint> committed = windowCheckpoints();
    return committed.stream().anyMatch(cp -> cp.snapshotId() == head);
  }

  /** The committed checkpoint of each window the CP currently reports for this index. */
  private List<WriteClient.ShardCheckpoint> windowCheckpoints() {
    var entry = controlPlane.getIndex(index);
    List<WriteClient.ShardCheckpoint> out = new ArrayList<>();
    for (var s : entry.getShardStatusList()) {
      if (s.getWindow() == 0 || s.getPrimary().isEmpty()) {
        continue;
      }
      WriteClient client = byEndpoint.computeIfAbsent(s.getPrimary(), WindowedWriteClient::connect);
      WriteClient.ShardCheckpoint cp = client.checkpoint(s.getWindow());
      if (cp != null) {
        out.add(cp); // an un-committed (just-placed) window doesn't constrain resume
      }
    }
    return out;
  }

  /** Parse a routable {@code [scheme://]host:port} endpoint into a {@link WriteClient}. */
  private static WriteClient connect(String endpoint) {
    String bare = endpoint.replaceFirst("^https?://", "");
    String[] hp = bare.split(":", 2);
    if (hp.length != 2) {
      throw new IllegalArgumentException("window owner endpoint must be host:port, got `" + endpoint + "`");
    }
    return new WriteClient(hp[0].trim(), Integer.parseInt(hp[1].trim()));
  }

  @Override
  public void close() throws InterruptedException {
    InterruptedException first = null;
    for (WriteClient client : byEndpoint.values()) {
      try {
        client.close();
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
