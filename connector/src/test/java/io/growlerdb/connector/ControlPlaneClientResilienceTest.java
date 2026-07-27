package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.growlerdb.proto.v1.ControlPlaneGrpc;
import io.growlerdb.proto.v1.GetIndexRequest;
import io.growlerdb.proto.v1.GetIndexResponse;
import io.growlerdb.proto.v1.ResolveUnitOwnerRequest;
import io.growlerdb.proto.v1.ResolveUnitOwnerResponse;
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
 * Resilience of {@link ControlPlaneClient} to a CP pod restart (HA-E3). {@code resolveWindowOwner}
 * sits on the windowed <b>write hot path</b>: a force-killed CP pod black-holes a blocking stub, so
 * without a per-call deadline ingestion freezes silently — and without retry, the brief
 * {@code UNAVAILABLE} of a leader failover fails the stream instead of being absorbed in place.
 * Mirrors {@link WriteClientResilienceTest}.
 */
class ControlPlaneClientResilienceTest {

  /** A wedged CP (handler never responds) must fail fast at the deadline, not hang forever. */
  @Test
  void getIndexFailsFastOnAHangingControlPlane() throws IOException {
    Server server = ServerBuilder.forPort(0).addService(new HangingControlPlane()).build().start();
    try {
      // 1s deadline, no retry: a black-holed call must surface DEADLINE_EXCEEDED, bounded.
      ControlPlaneClient client =
          new ControlPlaneClient("127.0.0.1", server.getPort(), 1, 1, 10, 10);
      assertTimeoutPreemptively(
          Duration.ofSeconds(10),
          () -> {
            StatusRuntimeException e =
                assertThrows(StatusRuntimeException.class, () -> client.getIndex("movies"));
            assertEquals(Status.Code.DEADLINE_EXCEEDED, e.getStatus().getCode());
          });
      closeQuietly(client);
    } finally {
      server.shutdownNow();
    }
  }

  /** A transient blip (one UNAVAILABLE — a leader failover behind the Service) is retried in place. */
  @Test
  void resolveWindowOwnerRetriesThroughATransientUnavailable() throws IOException {
    FlakyControlPlane handler = new FlakyControlPlane(/* failFirst= */ 1);
    Server server = ServerBuilder.forPort(0).addService(handler).build().start();
    try {
      ControlPlaneClient client =
          new ControlPlaneClient("127.0.0.1", server.getPort(), 5, 4, 20, 40);
      assertEquals("n1:50051", client.resolveWindowOwner("movies", 42L).getEndpoint());
      assertEquals(2, handler.calls.get()); // failed once, succeeded on retry
      closeQuietly(client);
    } finally {
      server.shutdownNow();
    }
  }

  /** A hash-shard resolve carries the ordinal on the {@code shard} selector (not {@code window}). */
  @Test
  void resolveShardOwnerSendsTheOrdinalShardSelector() throws IOException {
    CapturingControlPlane handler = new CapturingControlPlane();
    Server server = ServerBuilder.forPort(0).addService(handler).build().start();
    try {
      ControlPlaneClient client =
          new ControlPlaneClient("127.0.0.1", server.getPort(), 5, 4, 20, 40);
      assertEquals("n7:50051", client.resolveShardOwner("docs", 3).getEndpoint());
      assertEquals("docs", handler.last.getIndex());
      assertEquals(ResolveUnitOwnerRequest.UnitCase.SHARD, handler.last.getUnitCase());
      assertEquals(3, handler.last.getShard());
      closeQuietly(client);
    } finally {
      server.shutdownNow();
    }
  }

  /** An application error (unknown index) propagates immediately — retrying can't help. */
  @Test
  void getIndexDoesNotRetryAnApplicationError() throws IOException {
    RejectingControlPlane handler = new RejectingControlPlane();
    Server server = ServerBuilder.forPort(0).addService(handler).build().start();
    try {
      ControlPlaneClient client =
          new ControlPlaneClient("127.0.0.1", server.getPort(), 5, 4, 20, 40);
      StatusRuntimeException e =
          assertThrows(StatusRuntimeException.class, () -> client.getIndex("nope"));
      assertEquals(Status.Code.NOT_FOUND, e.getStatus().getCode());
      assertEquals(1, handler.calls.get()); // tried once, no retry
      closeQuietly(client);
    } finally {
      server.shutdownNow();
    }
  }

  private static void closeQuietly(ControlPlaneClient client) {
    try {
      client.close();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Never answers — models a CP whose network was torn down (force-killed pod). */
  private static final class HangingControlPlane extends ControlPlaneGrpc.ControlPlaneImplBase {
    @Override
    public void getIndex(GetIndexRequest request, StreamObserver<GetIndexResponse> obs) {
      // intentionally never onNext/onError — the client's deadline must cancel the call
    }
  }

  /** Fails the first {@code failFirst} resolves with UNAVAILABLE, then answers. */
  private static final class FlakyControlPlane extends ControlPlaneGrpc.ControlPlaneImplBase {
    final AtomicInteger calls = new AtomicInteger();
    private final int failFirst;

    FlakyControlPlane(int failFirst) {
      this.failFirst = failFirst;
    }

    @Override
    public void resolveUnitOwner(
        ResolveUnitOwnerRequest request, StreamObserver<ResolveUnitOwnerResponse> obs) {
      if (calls.incrementAndGet() <= failFirst) {
        obs.onError(Status.UNAVAILABLE.withDescription("leader failover").asRuntimeException());
        return;
      }
      obs.onNext(ResolveUnitOwnerResponse.newBuilder().setEndpoint("n1:50051").build());
      obs.onCompleted();
    }
  }

  /** Records the last resolve request, echoing a fixed owner — for asserting the unit selector. */
  private static final class CapturingControlPlane extends ControlPlaneGrpc.ControlPlaneImplBase {
    volatile ResolveUnitOwnerRequest last;

    @Override
    public void resolveUnitOwner(
        ResolveUnitOwnerRequest request, StreamObserver<ResolveUnitOwnerResponse> obs) {
      last = request;
      obs.onNext(ResolveUnitOwnerResponse.newBuilder().setEndpoint("n7:50051").build());
      obs.onCompleted();
    }
  }

  /** Always rejects with a non-retryable application error. */
  private static final class RejectingControlPlane extends ControlPlaneGrpc.ControlPlaneImplBase {
    final AtomicInteger calls = new AtomicInteger();

    @Override
    public void getIndex(GetIndexRequest request, StreamObserver<GetIndexResponse> obs) {
      calls.incrementAndGet();
      obs.onError(Status.NOT_FOUND.withDescription("no such index").asRuntimeException());
    }
  }
}
