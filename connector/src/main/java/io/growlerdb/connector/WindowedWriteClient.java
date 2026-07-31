package io.growlerdb.connector;

import io.growlerdb.proto.v1.DocBatch;
import io.growlerdb.proto.v1.DocOp;
import io.growlerdb.proto.v1.Value;
import io.growlerdb.proto.v1.WindowingConfig;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Streams a {@link DocBatch} into a <b>distributed windowed</b> index: each upsert is
 * routed to its <b>time window</b> ({@link WindowRouter}, byte-identical to the engine), the window's
 * owning node is resolved from the control plane (placed on first ask), and the window's sub-batch is
 * committed to that node — the write-side of CP-driven windowed placement, mirroring the engine's
 * batch {@code write_windowed}.
 *
 * <p>Unlike {@link ShardedWriteClient} (fixed ordinal shards, lockstep empty-batches), windows form
 * continuously and each advances independently: sub-batches carry <b>no</b> {@code from} checkpoint
 * (matching {@code TimeWindowing::partition_batch}, which uses {@code from = None}), so a window
 * that skipped a batch doesn't trip the node's continuity guard. The resume point is the min
 * committed checkpoint across the windows that have committed (idempotent replay; {@code batch_id}
 * dedups). The {@code safe} checkpoint (the connector's global resume floor) <b>is</b> carried, so
 * each window prunes its idempotency records instead of growing them without bound. Deletes carry
 * no window value, so they broadcast to every touched/known window (the owner re-broadcasts to its
 * own windows); append-mostly sources rarely delete.
 *
 * <p><b>Placement staleness.</b> The window → owner pin is a cache, not a lease: the CP can re-place
 * a window (its node died, or was deposed) while this process runs. When a window's write fails
 * transport-style past the write client's own retry budget, the pin is <b>invalidated</b> and the
 * owner re-resolved from the CP — once in place (the sub-batch retries onto the new owner; the
 * idempotent {@code batch_id} makes a replay of an already-committed write a no-op), and again on
 * the next batch if that also fails. Without this, a re-placed window's writes would hammer the old
 * endpoint for the process lifetime.
 */
public final class WindowedWriteClient implements BatchWriter {

  private final String index;
  private final ControlPlaneClient controlPlane;
  private final WindowRouter windowRouter;
  private final SnapshotLineage lineage;
  /** Endpoint → write client; injectable so tests can dial with tight deadlines/retries. */
  private final java.util.function.Function<String, WriteClient> dialer;
  /** window → the write client for its owning node (resolved from the CP, cached). */
  private final ConcurrentMap<Long, WriteClient> windowClient = new ConcurrentHashMap<>();
  /** node endpoint → write client (one channel per node, shared across its windows). */
  private final ConcurrentMap<String, WriteClient> byEndpoint = new ConcurrentHashMap<>();

  public WindowedWriteClient(
      String index, ControlPlaneClient controlPlane, WindowingConfig windowing, SnapshotLineage lineage) {
    this(index, controlPlane, windowing, lineage, e -> connect(e, index));
  }

  /** As above with an explicit {@code dialer} — used by tests to tighten the write retry budget. */
  WindowedWriteClient(
      String index,
      ControlPlaneClient controlPlane,
      WindowingConfig windowing,
      SnapshotLineage lineage,
      java.util.function.Function<String, WriteClient> dialer) {
    this.index = index;
    this.controlPlane = controlPlane;
    this.windowRouter = new WindowRouter(windowing);
    this.lineage = lineage;
    this.dialer = dialer;
  }

  @Override
  public long write(DocBatch batch) {
    long maxSnapshot = 0L;
    for (var entry : partition(batch, windowRouter, windowClient.keySet()).entrySet()) {
      maxSnapshot = Math.max(maxSnapshot, writeWindow(entry.getKey(), entry.getValue()));
    }
    return maxSnapshot;
  }

  /**
   * Commit one window's sub-batch to its owning node (tagged with this index so a pool node
   * dispatches it). A transport-class failure past {@link WriteClient}'s own retry budget invalidates
   * the window's pin and re-resolves the owner from the CP once: a re-placed window's retry lands on
   * the new owner (idempotent {@code batch_id} dedups the replay); otherwise the second failure
   * propagates with the pin dropped, so the NEXT batch re-resolves instead of staying wedged.
   */
  private long writeWindow(long window, DocBatch sub) {
    try {
      return clientForWindow(window).write(sub, index);
    } catch (StatusRuntimeException e) {
      if (!isPlacementSuspect(e.getStatus().getCode())) {
        throw e; // an application error — re-resolving placement can't help
      }
      windowClient.remove(window);
      try {
        return clientForWindow(window).write(sub, index);
      } catch (StatusRuntimeException again) {
        if (isPlacementSuspect(again.getStatus().getCode())) {
          windowClient.remove(window);
        }
        throw again;
      }
    }
  }

  /**
   * Transport-class failures that may mean "this window's pinned owner is gone" (as opposed to an
   * application rejection, where the placement is fine and a re-resolve is noise).
   */
  private static boolean isPlacementSuspect(Status.Code code) {
    return code == Status.Code.UNAVAILABLE
        || code == Status.Code.DEADLINE_EXCEEDED
        || code == Status.Code.CANCELLED;
  }

  /**
   * Split {@code batch} into one sub-batch per time window, routing each upsert by its window field
   * ({@link WindowRouter}) and broadcasting deletes to every touched window plus every {@code
   * knownWindow} (a delete carries no window value; the owner re-broadcasts to its own windows).
   * Each sub-batch carries the same checkpoint, the same {@code safe} resume floor (global across
   * windows, so each prunes its idempotency records), and a per-window {@code batch_id}
   * ({@code {id}#w{window}}) — and, matching {@code TimeWindowing::partition_batch}, <b>no</b>
   * {@code from} checkpoint, so a window that skipped a batch isn't gap-rejected by the node's
   * continuity guard. Pure (no I/O), so the placement is unit-tested without a live cluster.
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
                    + "` — add it to --fields so the connector can route by window");
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
      DocBatch.Builder sub =
          DocBatch.newBuilder()
              .addAllOps(ops)
              .setCheckpoint(batch.getCheckpoint())
              .setBatchId(batch.getBatchId() + "#w" + window);
      if (batch.hasSafeCheckpoint()) {
        sub.setSafeCheckpoint(batch.getSafeCheckpoint());
      }
      out.put(window, sub.build());
    }
    return out;
  }

  /** The write client for a window's owning node — resolved from the CP (placed on first ask), cached. */
  private WriteClient clientForWindow(long window) {
    return windowClient.computeIfAbsent(
        window,
        w -> {
          String endpoint = controlPlane.resolveWindowOwner(index, w).getEndpoint();
          return byEndpoint.computeIfAbsent(endpoint, dialer);
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
      WriteClient client = byEndpoint.computeIfAbsent(s.getPrimary(), dialer);
      WriteClient.ShardCheckpoint cp = client.checkpoint(s.getWindow(), index);
      if (cp != null) {
        out.add(cp); // an un-committed (just-placed) window doesn't constrain resume
      }
    }
    return out;
  }

  /** Parse a routable {@code [scheme://]host:port} endpoint into a {@link WriteClient}. */
  private static WriteClient connect(String endpoint, String index) {
    String bare = endpoint.replaceFirst("^https?://", "");
    String[] hp = bare.split(":", 2);
    if (hp.length != 2) {
      throw new IllegalArgumentException("window owner endpoint must be host:port, got `" + endpoint + "`");
    }
    return new WriteClient(hp[0].trim(), Integer.parseInt(hp[1].trim()), index);
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
