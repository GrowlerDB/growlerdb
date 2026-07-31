package io.growlerdb.connector;

import io.growlerdb.proto.v1.ControlPlaneGrpc;
import io.growlerdb.proto.v1.GetIndexRequest;
import io.growlerdb.proto.v1.GetIndexResponse;
import io.growlerdb.proto.v1.ResolveUnitOwnerRequest;
import io.growlerdb.proto.v1.ResolveUnitOwnerResponse;
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
 * Thin client to the GrowlerDB {@code ControlPlane} gRPC service. The connector uses it
 * to fetch an index's <b>routing config</b> from the registry — the same source the Gateway reads
 * from — so write placement and read routing can't drift. {@link #getIndex} returns the
 * shard count and routing strategy; the connector validates its own endpoint set against them and
 * fails fast on a mismatch instead of silently writing where reads never look.
 *
 * <p><b>Resilience to a CP restart.</b> {@link #resolveWindowOwner} sits on the windowed write hot
 * path, so this client carries the same two guards as {@link WriteClient}: a <b>per-call deadline</b>
 * plus channel keepalive (a force-killed CP pod black-holes a blocking stub; without a deadline
 * ingestion freezes silently on TCP retransmits), and a <b>bounded retry with backoff</b> for {@code
 * UNAVAILABLE}/{@code DEADLINE_EXCEEDED} — the CP is behind a Service, so a leader failover lands the
 * retry on the new pod. Both RPCs are idempotent, so a retry never double-places.
 */
public final class ControlPlaneClient implements AutoCloseable {

  /** Metadata key carrying the shared service token — matches the Rust control plane's. */
  private static final Metadata.Key<String> SERVICE_TOKEN_KEY =
      Metadata.Key.of("x-growlerdb-service-token", Metadata.ASCII_STRING_MARSHALLER);

  /**
   * Per-call deadline. CP calls are small metadata lookups (far below {@link
   * WriteClient#PER_CALL_DEADLINE_SECONDS}'s large-batch allowance), so 10s is generous while still
   * bounding a black-holed call to seconds, not TCP-retransmit forever.
   */
  static final int PER_CALL_DEADLINE_SECONDS = 10;

  /** Retry tunables: enough capped backoff (~5 attempts to 5s) to cover a CP leader failover. */
  static final int DEFAULT_MAX_ATTEMPTS = 5;

  static final long DEFAULT_INITIAL_BACKOFF_MS = 500;
  static final long DEFAULT_MAX_BACKOFF_MS = 5_000;

  private final ManagedChannel channel;
  private final ControlPlaneGrpc.ControlPlaneBlockingStub stub;
  private final int deadlineSeconds;
  private final int maxAttempts;
  private final long initialBackoffMs;
  private final long maxBackoffMs;

  /**
   * Connect to the Control Plane at {@code host:port} (plaintext). When {@code
   * GROWLERDB_SERVICE_TOKEN} is set, every call carries it as the {@code x-growlerdb-service-token}
   * header so a closed control plane authenticates the connector; unset ⇒ no header (open dev). TLS
   * to the control plane is not yet wired here.
   */
  public ControlPlaneClient(String host, int port) {
    this(
        host,
        port,
        PER_CALL_DEADLINE_SECONDS,
        DEFAULT_MAX_ATTEMPTS,
        DEFAULT_INITIAL_BACKOFF_MS,
        DEFAULT_MAX_BACKOFF_MS);
  }

  /** As above with explicit deadline/retry tunables — used by tests to exercise the fast path. */
  ControlPlaneClient(
      String host,
      int port,
      int deadlineSeconds,
      int maxAttempts,
      long initialBackoffMs,
      long maxBackoffMs) {
    if (maxAttempts < 1) {
      throw new IllegalArgumentException("maxAttempts must be >= 1"); // else callWithRetry NPEs
    }
    // dns:/// + keepalive, mirroring WriteClient: an unanswered ping to a force-killed CP pod trips
    // the subchannel to TRANSIENT_FAILURE → DNS re-resolution → reconnect to the Service's live pod,
    // so the retry below lands somewhere alive.
    this.channel =
        ManagedChannelBuilder.forTarget("dns:///" + host + ":" + port)
            .usePlaintext()
            .keepAliveTime(10, TimeUnit.SECONDS)
            .keepAliveTimeout(5, TimeUnit.SECONDS)
            .keepAliveWithoutCalls(true)
            .build();
    ControlPlaneGrpc.ControlPlaneBlockingStub s = ControlPlaneGrpc.newBlockingStub(channel);
    String token = System.getenv("GROWLERDB_SERVICE_TOKEN");
    if (token != null && !token.isEmpty()) {
      Metadata md = new Metadata();
      md.put(SERVICE_TOKEN_KEY, token);
      ClientInterceptor interceptor = MetadataUtils.newAttachHeadersInterceptor(md);
      s = s.withInterceptors(interceptor);
    }
    this.stub = s;
    this.deadlineSeconds = deadlineSeconds;
    this.maxAttempts = maxAttempts;
    this.initialBackoffMs = initialBackoffMs;
    this.maxBackoffMs = maxBackoffMs;
  }

  /** The registry's routing config for {@code index} (shard count + strategy, + windowing config). */
  public GetIndexResponse getIndex(String index) {
    return callWithRetry(
        "getIndex", () -> deadlined().getIndex(GetIndexRequest.newBuilder().setName(index).build()));
  }

  /**
   * Resolve the node that owns a time {@code window} of a windowed {@code index}, placing
   * it on the least-loaded live node on first ask. The connector calls this with each row's computed
   * window id to learn where to stream that window's writes. Idempotent for an already-placed window.
   */
  public ResolveUnitOwnerResponse resolveWindowOwner(String index, long window) {
    return callWithRetry(
        "resolveWindowOwner",
        () ->
            deadlined()
                .resolveUnitOwner(
                    ResolveUnitOwnerRequest.newBuilder().setIndex(index).setWindow(window).build()));
  }

  /**
   * Resolve the node that owns ordinal {@code shard} of a hash/partition-sharded {@code index},
   * placing it on the least-loaded live node on first ask — the hash counterpart to {@link
   * #resolveWindowOwner}. In the placement pool (D52) a hash index's ordinals are CP-placed, so the
   * connector resolves each owned ordinal's current pool node this way. Idempotent for an
   * already-placed shard (returns its existing primary).
   */
  public ResolveUnitOwnerResponse resolveShardOwner(String index, int shard) {
    return callWithRetry(
        "resolveShardOwner",
        () ->
            deadlined()
                .resolveUnitOwner(
                    ResolveUnitOwnerRequest.newBuilder().setIndex(index).setShard(shard).build()));
  }

  /** A fresh stub bearing this attempt's deadline (deadlines are absolute, so set per attempt). */
  private ControlPlaneGrpc.ControlPlaneBlockingStub deadlined() {
    return stub.withDeadlineAfter(deadlineSeconds, TimeUnit.SECONDS);
  }

  /**
   * Run {@code call}, retrying transient transport failures with capped exponential backoff —
   * the CP is behind a Service, so retrying is the correct reaction to a leader failover. Only
   * {@code UNAVAILABLE}/{@code DEADLINE_EXCEEDED} are retried; application errors (e.g. an unknown
   * index) propagate immediately.
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
        System.err.printf(
            "ControlPlaneClient.%s failed (%s), attempt %d/%d — retrying in %dms%n",
            op, e.getStatus().getCode(), attempt, maxAttempts, backoffMs);
        sleep(backoffMs);
        backoffMs = Math.min(backoffMs * 2, maxBackoffMs);
      }
    }
    // Unreachable (maxAttempts >= 1, guarded in the ctor): the last attempt rethrows above.
    throw new AssertionError("callWithRetry exhausted without returning or throwing");
  }

  private static boolean isRetryable(Status.Code code) {
    return code == Status.Code.UNAVAILABLE || code == Status.Code.DEADLINE_EXCEEDED;
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
