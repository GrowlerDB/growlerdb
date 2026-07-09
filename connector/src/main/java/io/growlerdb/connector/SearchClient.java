package io.growlerdb.connector;

import io.growlerdb.proto.v1.SearchGrpc;
import io.growlerdb.proto.v1.SearchHit;
import io.growlerdb.proto.v1.SearchRequest;
import io.growlerdb.proto.v1.SearchResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Thin client to a GrowlerDB <b>read</b> endpoint's {@code Search} gRPC service — a Node
 * ({@code growlerdb serve}) or a Gateway ({@code growlerdb gateway}, which fans the query out
 * across shards/windows). Powers the SQL UDFs (task-51): an engine runs a boolean/full-text query
 * and gets back matching document <b>coordinates</b> (the composite keys, D5) + scores, then
 * <b>joins those keys against the Iceberg table</b> to filter/score its rows ({@link GrowlerDbSearch}).
 *
 * <p>The endpoint is the index — a Gateway is started per index ({@code gateway --index}) — so the
 * query carries no index name. Plaintext only; TLS/auth on this read path are a follow-up.
 *
 * <p><b>Resilience (task-152 / F12).</b> The read path mirrors {@link WriteClient}'s guards so a
 * wedged Node/Gateway can't hang the query thread forever: a {@link #keepAliveTime keepalive} trips a
 * black-holed connection to re-resolve DNS, a <b>per-call deadline</b> fails a stuck RPC fast, and
 * transient transport failures are <b>retried with backoff</b> (a read is idempotent, so retry is
 * always safe). {@link #maxInboundMessageSize} lets a wide hit list through instead of failing the
 * SQL query with {@code RESOURCE_EXHAUSTED}.
 */
public final class SearchClient implements AutoCloseable {

  /** gRPC inbound cap — a broad query can return a large hit list; don't fail on the 4 MiB default. */
  static final int MAX_MESSAGE_BYTES = 256 * 1024 * 1024;

  /** Per-call deadline default; overridable via {@code GROWLERDB_SEARCH_DEADLINE_SECONDS}. */
  static final int PER_CALL_DEADLINE_SECONDS = 30;

  static final int DEFAULT_MAX_ATTEMPTS = 5;
  static final long DEFAULT_INITIAL_BACKOFF_MS = 500;
  static final long DEFAULT_MAX_BACKOFF_MS = 5_000;

  private final ManagedChannel channel;
  private final SearchGrpc.SearchBlockingStub stub;
  private final int deadlineSeconds;
  private final int maxAttempts;
  private final long initialBackoffMs;
  private final long maxBackoffMs;

  /** Connect to a GrowlerDB read endpoint at {@code host:port}. */
  public SearchClient(String host, int port) {
    this(
        host,
        port,
        deadlineSecondsFromEnv(),
        DEFAULT_MAX_ATTEMPTS,
        DEFAULT_INITIAL_BACKOFF_MS,
        DEFAULT_MAX_BACKOFF_MS);
  }

  /** Per-call deadline, {@code GROWLERDB_SEARCH_DEADLINE_SECONDS} or {@link #PER_CALL_DEADLINE_SECONDS}. */
  static int deadlineSecondsFromEnv() {
    String v = System.getenv("GROWLERDB_SEARCH_DEADLINE_SECONDS");
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

  /** As above with explicit deadline/retry tunables — used by tests. */
  SearchClient(
      String host,
      int port,
      int deadlineSeconds,
      int maxAttempts,
      long initialBackoffMs,
      long maxBackoffMs) {
    if (maxAttempts < 1) {
      throw new IllegalArgumentException("maxAttempts must be >= 1");
    }
    // dns:/// so a restarted endpoint's new pod IP is re-resolved on reconnect; keepalive trips a
    // black-holed connection to TRANSIENT_FAILURE → re-resolution (see WriteClient for the detail).
    this.channel =
        ManagedChannelBuilder.forTarget("dns:///" + host + ":" + port)
            .usePlaintext()
            .maxInboundMessageSize(MAX_MESSAGE_BYTES)
            .keepAliveTime(10, TimeUnit.SECONDS)
            .keepAliveTimeout(5, TimeUnit.SECONDS)
            .keepAliveWithoutCalls(true)
            .build();
    this.stub = SearchGrpc.newBlockingStub(channel);
    this.deadlineSeconds = deadlineSeconds;
    this.maxAttempts = maxAttempts;
    this.initialBackoffMs = initialBackoffMs;
    this.maxBackoffMs = maxBackoffMs;
  }

  /**
   * Run {@code query} (Lucene/KQL boolean retrieval over the index's postings) and return up to
   * {@code limit} ranked hits — each a key + score. The caller projects the keys into columns and
   * joins them against the source Iceberg table.
   */
  public List<SearchHit> search(String query, int limit) {
    SearchRequest request = SearchRequest.newBuilder().setQuery(query).setLimit(limit).build();
    SearchResponse response = callWithRetry("search", () -> deadlined().search(request));
    return response.getHitsList();
  }

  /** A fresh stub bearing this attempt's absolute deadline. */
  private SearchGrpc.SearchBlockingStub deadlined() {
    return stub.withDeadlineAfter(deadlineSeconds, TimeUnit.SECONDS);
  }

  /**
   * Run {@code call}, retrying transient transport failures ({@code UNAVAILABLE}/{@code
   * DEADLINE_EXCEEDED}) with capped exponential backoff. Reads are idempotent, so retry is always
   * safe; application errors (e.g. an invalid query) propagate immediately.
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
            "SearchClient.%s failed (%s), attempt %d/%d — retrying in %dms%n",
            op, e.getStatus().getCode(), attempt, maxAttempts, backoffMs);
        sleep(backoffMs);
        backoffMs = Math.min(backoffMs * 2, maxBackoffMs);
      }
    }
    throw new AssertionError("unreachable: the last attempt rethrows");
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
