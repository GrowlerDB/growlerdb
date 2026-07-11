package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.growlerdb.proto.v1.SearchGrpc;
import io.growlerdb.proto.v1.SearchRequest;
import io.growlerdb.proto.v1.SearchResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Resilience of the read-path {@link SearchClient}, the mirror of
 * {@link WriteClientResilienceTest}. A wedged Node/Gateway must fail fast at the deadline instead of
 * hanging the query thread, and a transient blip must be retried (a read is idempotent).
 */
class SearchClientResilienceTest {

  /** A wedged endpoint (handler never responds) must fail fast at the deadline, not hang. */
  @Test
  void searchFailsFastOnAHangingEndpointInsteadOfBlockingForever() throws IOException {
    Server server = ServerBuilder.forPort(0).addService(new HangingSearch()).build().start();
    try {
      // 1s deadline, no retry → a wedged endpoint surfaces DEADLINE_EXCEEDED, bounded.
      SearchClient client = new SearchClient("127.0.0.1", server.getPort(), 1, 1, 10, 10);
      assertTimeoutPreemptively(
          Duration.ofSeconds(10),
          () -> {
            StatusRuntimeException e =
                assertThrows(StatusRuntimeException.class, () -> client.search("body:x", 10));
            assertEquals(Status.Code.DEADLINE_EXCEEDED, e.getStatus().getCode());
          });
      closeQuietly(client);
    } finally {
      server.shutdownNow();
    }
  }

  /** A transient blip (one UNAVAILABLE, then up) is absorbed by the retry — no error escapes. */
  @Test
  void searchRetriesThroughATransientUnavailable() throws IOException {
    FlakySearch handler = new FlakySearch(/* failFirst= */ 1);
    Server server = ServerBuilder.forPort(0).addService(handler).build().start();
    try {
      SearchClient client = new SearchClient("127.0.0.1", server.getPort(), 5, 4, 20, 40);
      assertEquals(1, client.search("body:x", 10).size());
      assertEquals(2, handler.calls.get()); // failed once, succeeded on retry
      closeQuietly(client);
    } finally {
      server.shutdownNow();
    }
  }

  private static void closeQuietly(SearchClient client) {
    try {
      client.close();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static final class HangingSearch extends SearchGrpc.SearchImplBase {
    @Override
    public void search(SearchRequest request, StreamObserver<SearchResponse> obs) {
      // never responds — the client's deadline must cancel the call
    }
  }

  private static final class FlakySearch extends SearchGrpc.SearchImplBase {
    final AtomicInteger calls = new AtomicInteger();
    private final int failFirst;

    FlakySearch(int failFirst) {
      this.failFirst = failFirst;
    }

    @Override
    public void search(SearchRequest request, StreamObserver<SearchResponse> obs) {
      if (calls.incrementAndGet() <= failFirst) {
        obs.onError(Status.UNAVAILABLE.withDescription("endpoint restarting").asRuntimeException());
        return;
      }
      obs.onNext(
          SearchResponse.newBuilder()
              .addHits(io.growlerdb.proto.v1.SearchHit.newBuilder().setScore(1.0))
              .setTotal(1)
              .build());
      obs.onCompleted();
    }
  }
}
