package io.growlerdb.trino;

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
 * Thin client to a GrowlerDB read endpoint's {@code Search} gRPC service — a Node or, more usually,
 * a Gateway ({@code growlerdb gateway --index}, which fans the query across shards/windows). Powers
 * the Trino {@code growlerdb_search} table function: run a boolean query, get matching keys + scores
 * back to JOIN against the Iceberg table. Mirrors the Spark connector's SearchClient.
 *
 * <p><b>Resilience:</b> a wedged endpoint must not hang the Trino split thread forever. The channel
 * sets keepalive + a large inbound cap, every call carries a per-call deadline, and transient
 * transport failures are retried with backoff (a read is idempotent).
 */
public final class SearchClient implements AutoCloseable {

  static final int MAX_MESSAGE_BYTES = 256 * 1024 * 1024;
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

  public SearchClient(String host, int port) {
    this(
        host,
        port,
        deadlineSecondsFromEnv(),
        DEFAULT_MAX_ATTEMPTS,
        DEFAULT_INITIAL_BACKOFF_MS,
        DEFAULT_MAX_BACKOFF_MS);
  }

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

  /** Run {@code query} (Lucene/KQL boolean retrieval) and return up to {@code limit} ranked hits. */
  public List<SearchHit> search(String query, int limit) {
    SearchRequest request = SearchRequest.newBuilder().setQuery(query).setLimit(limit).build();
    SearchResponse response = callWithRetry("search", () -> deadlined().search(request));
    return response.getHitsList();
  }

  private SearchGrpc.SearchBlockingStub deadlined() {
    return stub.withDeadlineAfter(deadlineSeconds, TimeUnit.SECONDS);
  }

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
