package io.growlerdb.connector;

import io.growlerdb.proto.v1.DocBatch;
import io.growlerdb.proto.v1.GetCheckpointRequest;
import io.growlerdb.proto.v1.GetCheckpointResponse;
import io.growlerdb.proto.v1.SourceCheckpoint;
import io.growlerdb.proto.v1.WriteGrpc;
import io.growlerdb.proto.v1.WriteRequest;
import io.growlerdb.proto.v1.WriteResponse;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Thin client to a GrowlerDB Node's {@code Write} gRPC service ({@code growlerdb serve}). The
 * connector maps changelog rows to {@link DocBatch}es and commits them through this client.
 *
 * <p><b>Resilience to a Node restart.</b> The
 * channel resolves the Node by DNS ({@code dns:///host:port}), so when a shard's pod crashes and
 * returns at a new IP the name is re-resolved on reconnect. But that only helps if a call to the
 * dead pod actually <i>fails</i> — a force-killed pod's torn-down network black-holes in-flight
 * requests, and a blocking RPC with no deadline waits on TCP retransmits ~forever, freezing
 * ingestion with no error (so the connector's "exit on stream error → auto-restart → resume"
 * safety net never fires). Two guards prevent that:
 *
 * <ul>
 *   <li>every call carries a <b>per-call deadline</b> ({@value #PER_CALL_DEADLINE_SECONDS}s) so a
 *       wedged Node fails fast ({@code DEADLINE_EXCEEDED}) instead of hanging; and
 *   <li>transient transport failures ({@code UNAVAILABLE}/{@code DEADLINE_EXCEEDED}) are
 *       <b>retried with backoff</b>, giving the channel time to re-resolve DNS and reconnect to
 *       the returned pod so the write resumes <i>in place</i>. Retries are safe because every
 *       batch carries an idempotent {@code batch_id} the Node dedups, so a replay of a
 *       write that actually committed is a no-op.
 * </ul>
 *
 * <p>If a Node stays down past the retry budget the last failure propagates, the streaming query
 * fails, and the connector pod auto-restarts and resumes exactly-once from each Node's committed
 * checkpoint — the freeze becomes a bounded, self-healing blip either way.
 */
public final class WriteClient implements BatchWriter {

  /**
   * gRPC inbound message cap, matching the Node's raised {@code Write} decode limit. The
   * connector mostly <i>sends</i> (and bounded catch-up keeps each batch small), so this is a safety
   * margin for large responses rather than the hot path — but it keeps both ends consistent.
   */
  static final int MAX_MESSAGE_BYTES = 256 * 1024 * 1024;

  /**
   * Per-call deadline default. Bounds any single RPC so a wedged Node fails fast rather
   * than blocking on TCP retransmits. 30s is generous for a large bounded-catch-up sub-batch (up to
   * {@code DEFAULT_MAX_COMMIT_ROWS}); a steady stream of tiny batches can run much tighter for
   * faster in-place recovery via {@code GROWLERDB_WRITE_DEADLINE_SECONDS}.
   */
  static final int PER_CALL_DEADLINE_SECONDS = 30;

  /** Retry tunables: capped exponential backoff over transient transport failures. The
   *  budget is generous so a FULL node roll — all pods restarting with new IPs, dials to
   *  stale IPs timing out — is absorbed here (retry until DNS re-resolves + a pod is READY) before
   *  the micro-batch fails; if it still exhausts, the streaming query restarts in-process rather than
   *  the JVM exiting. ~10 attempts with backoff to 15s ≈ a couple minutes of coverage. */
  static final int DEFAULT_MAX_ATTEMPTS = 10;

  static final long DEFAULT_INITIAL_BACKOFF_MS = 1_000;
  static final long DEFAULT_MAX_BACKOFF_MS = 15_000;

  private final ManagedChannel channel;
  private final WriteGrpc.WriteBlockingStub stub;
  private final int deadlineSeconds;
  private final int maxAttempts;
  private final long initialBackoffMs;
  private final long maxBackoffMs;

  /** Connect to a GrowlerDB Node at {@code host:port} (plaintext; TLS/auth are future work). */
  public WriteClient(String host, int port) {
    this(
        host,
        port,
        deadlineSecondsFromEnv(),
        DEFAULT_MAX_ATTEMPTS,
        DEFAULT_INITIAL_BACKOFF_MS,
        DEFAULT_MAX_BACKOFF_MS);
  }

  /** Per-call deadline, {@code GROWLERDB_WRITE_DEADLINE_SECONDS} or {@link #PER_CALL_DEADLINE_SECONDS}. */
  static int deadlineSecondsFromEnv() {
    String v = System.getenv("GROWLERDB_WRITE_DEADLINE_SECONDS");
    if (v != null && !v.isBlank()) {
      try {
        int n = Integer.parseInt(v.trim());
        if (n > 0) {
          return n;
        }
      } catch (NumberFormatException ignored) {
        // fall through to the default
      }
    }
    return PER_CALL_DEADLINE_SECONDS;
  }

  /** As above with explicit deadline/retry tunables — used by tests to exercise the fast path. */
  WriteClient(
      String host,
      int port,
      int deadlineSeconds,
      int maxAttempts,
      long initialBackoffMs,
      long maxBackoffMs) {
    if (maxAttempts < 1) {
      throw new IllegalArgumentException("maxAttempts must be >= 1"); // else callWithRetry NPEs
    }
    // Transport-agnostic at compile time; grpc-netty-shaded supplies it at runtime. dns:/// so a
    // restarted Node's new pod IP is re-resolved on reconnect.
    //
    // Keepalive is what makes that reconnect happen *in place*. A force-killed Node pod
    // black-holes its TCP connection: without keepalive the subchannel stays READY (gRPC never
    // learns the socket is dead), a call just hits its deadline, and the channel never re-resolves
    // DNS — so every retry re-uses the dead connection until the budget exhausts and the connector
    // restarts. With keepalive, an unanswered ping (10s interval, 5s ack timeout) trips the
    // subchannel to TRANSIENT_FAILURE → DNS re-resolution → reconnect to the returned pod's new IP,
    // so writes resume without a pod restart. Pair with a low JVM DNS TTL (connector.yaml) so the
    // re-resolution returns the new IP. keepAliveWithoutCalls so a dead idle connection is caught too.
    this.channel =
        ManagedChannelBuilder.forTarget("dns:///" + host + ":" + port)
            .usePlaintext()
            .maxInboundMessageSize(MAX_MESSAGE_BYTES)
            .keepAliveTime(10, TimeUnit.SECONDS)
            .keepAliveTimeout(5, TimeUnit.SECONDS)
            .keepAliveWithoutCalls(true)
            .build();
    // When GROWLERDB_SERVICE_TOKEN is set, every call carries it as the shared
    // x-growlerdb-service-token header — the Node's data plane enforces the same token when
    // configured (unset => no header, open dev). Mirrors ControlPlaneClient.
    WriteGrpc.WriteBlockingStub s = WriteGrpc.newBlockingStub(channel);
    String token = System.getenv("GROWLERDB_SERVICE_TOKEN");
    if (token != null && !token.isEmpty()) {
      Metadata md = new Metadata();
      md.put(
          Metadata.Key.of("x-growlerdb-service-token", Metadata.ASCII_STRING_MARSHALLER), token);
      ClientInterceptor interceptor = MetadataUtils.newAttachHeadersInterceptor(md);
      s = s.withInterceptors(interceptor);
    }
    this.stub = s;
    this.deadlineSeconds = deadlineSeconds;
    this.maxAttempts = maxAttempts;
    this.initialBackoffMs = initialBackoffMs;
    this.maxBackoffMs = maxBackoffMs;
  }

  /** Commit a batch; returns the committed index snapshot. */
  public long write(DocBatch batch) {
    WriteRequest request = WriteRequest.newBuilder().setBatch(batch).build();
    WriteResponse response = callWithRetry("write", () -> deadlined().write(request));
    return response.getSnapshot();
  }

  /**
   * A shard's durably committed checkpoint: the snapshot id (identity — a random long) plus,
   * when known, its lineage-monotone Iceberg sequence number, which is the only sound way to order
   * two checkpoints.
   */
  public record ShardCheckpoint(long snapshotId, java.util.OptionalLong sequenceNumber) {}

  /**
   * The Iceberg snapshot the Node has durably committed, or {@code null} if it has
   * none yet — the connector's <b>resume point</b> after a restart. Because
   * the write and checkpoint commit atomically, this never points past applied data;
   * a window read from here is at-least-once and de-duplicated by {@code batch_id}.
   */
  public Long checkpointSnapshotId() {
    ShardCheckpoint cp = checkpoint();
    return cp == null ? null : cp.snapshotId();
  }

  /** As {@link #checkpointSnapshotId()}, with the sequence number when the Node has one. */
  public ShardCheckpoint checkpoint() {
    return checkpoint(0L);
  }

  /**
   * The checkpoint for a specific time {@code window} on a windowed node — the connector
   * resumes each window independently. {@code window == 0} reads the node's single (ordinal) shard.
   */
  public ShardCheckpoint checkpoint(long window) {
    GetCheckpointResponse response =
        callWithRetry(
            "getCheckpoint",
            () ->
                deadlined()
                    .getCheckpoint(GetCheckpointRequest.newBuilder().setWindow(window).build()));
    if (!response.hasCheckpoint()) {
      return null;
    }
    SourceCheckpoint cp = response.getCheckpoint();
    if (cp.getKindCase() != SourceCheckpoint.KindCase.ICEBERG_SNAPSHOT) {
      return null;
    }
    long seq = cp.getIcebergSequenceNumber();
    return new ShardCheckpoint(
        cp.getIcebergSnapshot(),
        seq > 0 ? java.util.OptionalLong.of(seq) : java.util.OptionalLong.empty());
  }

  /** Drained when this shard's durable checkpoint has reached exactly {@code head}. */
  @Override
  public boolean drainedTo(long head) {
    Long cp = checkpointSnapshotId();
    return cp != null && cp == head;
  }

  /** A fresh stub bearing this attempt's deadline (deadlines are absolute, so set per attempt). */
  private WriteGrpc.WriteBlockingStub deadlined() {
    return stub.withDeadlineAfter(deadlineSeconds, TimeUnit.SECONDS);
  }

  /**
   * Run {@code call}, retrying transient transport failures with capped exponential backoff so a
   * brief Node outage (a crashed pod returning at a new IP) is absorbed in place. Only
   * {@code UNAVAILABLE}/{@code DEADLINE_EXCEEDED} are retried — application errors (e.g. the
   * lineage guard) propagate immediately. Idempotent {@code batch_id} makes a write replay safe.
   */
  private <T> T callWithRetry(String op, Supplier<T> call) {
    long backoffMs = initialBackoffMs;
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        return call.get();
      } catch (StatusRuntimeException e) {
        if (!isRetryable(e.getStatus().getCode()) || attempt == maxAttempts) {
          throw e;
        }
        // Count the retry so a transient-failure storm shows up as a metric, not just in the printf.
        ConnectorMetrics.recordWriteRetry(e.getStatus().getCode().name());
        System.err.printf(
            "WriteClient.%s failed (%s), attempt %d/%d — retrying in %dms%n",
            op, e.getStatus().getCode(), attempt, maxAttempts, backoffMs);
        sleep(backoffMs);
        backoffMs = Math.min(backoffMs * 2, maxBackoffMs);
      }
    }
    // Unreachable (maxAttempts >= 1, guarded in the ctor): the last attempt rethrows above.
    throw new AssertionError("callWithRetry exhausted without returning or throwing");
  }

  private static boolean isRetryable(Status.Code code) {
    // RESOURCE_EXHAUSTED is transient by construction: it is the Node's write-admission
    // backpressure — a slot is busy, not a permanent rejection. Under a compaction I/O storm a slow
    // commit can hold its admission slot past the client deadline (the Node bounds concurrent commits
    // to the slot count so it sheds load rather than thrashing the disk), so a retry hits
    // RESOURCE_EXHAUSTED. Treating it as non-retryable would turn that backpressure into an instant
    // stream failure, so it must back off and retry. The idempotent batch_id makes the eventual
    // replay a safe no-op.
    return code == Status.Code.UNAVAILABLE
        || code == Status.Code.DEADLINE_EXCEEDED
        || code == Status.Code.RESOURCE_EXHAUSTED;
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new StatusRuntimeException(Status.CANCELLED.withDescription("retry interrupted"));
    }
  }

  @Override
  public void close() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }
}
