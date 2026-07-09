package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.growlerdb.proto.v1.DocBatch;
import io.growlerdb.proto.v1.WriteGrpc;
import io.growlerdb.proto.v1.WriteRequest;
import io.growlerdb.proto.v1.WriteResponse;
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
 * Resilience of the connector's {@link WriteClient} to a Node restart (task-124, the Java sibling
 * of the Gateway's task-122). A force-killed shard pod black-holes in-flight writes; without a
 * deadline the blocking RPC hangs ~forever and ingestion freezes silently. These exercise the two
 * guards against that — a per-call deadline (fail fast, don't hang) and idempotent retry with
 * backoff (absorb a brief outage in place) — against an in-process Node stub, no Spark needed.
 */
class WriteClientResilienceTest {

  private static final DocBatch BATCH =
      DocBatch.newBuilder().setBatchId("b1").build();

  /** A wedged Node (handler never responds) must fail fast at the deadline, not hang. */
  @Test
  void writeFailsFastOnAHangingNodeInsteadOfBlockingForever() throws IOException {
    Server server =
        ServerBuilder.forPort(0).addService(new HangingWrite()).build().start();
    try {
      // 1s deadline, no retry: a wedged Node should surface DEADLINE_EXCEEDED, bounded.
      WriteClient client = new WriteClient("127.0.0.1", server.getPort(), 1, 1, 10, 10);
      assertTimeoutPreemptively(
          Duration.ofSeconds(10),
          () -> {
            StatusRuntimeException e =
                assertThrows(StatusRuntimeException.class, () -> client.write(BATCH));
            assertEquals(Status.Code.DEADLINE_EXCEEDED, e.getStatus().getCode());
          });
      closeQuietly(client);
    } finally {
      server.shutdownNow();
    }
  }

  /** A transient blip (one UNAVAILABLE, then up) is absorbed in place — no error escapes. */
  @Test
  void writeRetriesThroughATransientUnavailable() throws IOException {
    FlakyWrite handler = new FlakyWrite(/* failFirst= */ 1, /* snapshot= */ 77);
    Server server = ServerBuilder.forPort(0).addService(handler).build().start();
    try {
      WriteClient client = new WriteClient("127.0.0.1", server.getPort(), 5, 4, 20, 40);
      assertEquals(77L, client.write(BATCH));
      assertEquals(2, handler.calls.get()); // failed once, succeeded on retry
      closeQuietly(client);
    } finally {
      server.shutdownNow();
    }
  }

  /**
   * RESOURCE_EXHAUSTED is the Node's write-admission backpressure — transient by construction
   * (task-194): a slow commit under a compaction I/O storm holds its admission slot, so a retry can
   * hit it. It must be retried (the idempotent batch_id makes the replay safe), not turned into an
   * instant stream failure (the detonator of the silent-loss event).
   */
  @Test
  void writeRetriesThroughResourceExhaustedBackpressure() throws IOException {
    FlakyWrite handler =
        new FlakyWrite(/* failFirst= */ 1, /* snapshot= */ 88, Status.RESOURCE_EXHAUSTED);
    Server server = ServerBuilder.forPort(0).addService(handler).build().start();
    try {
      WriteClient client = new WriteClient("127.0.0.1", server.getPort(), 5, 4, 20, 40);
      assertEquals(88L, client.write(BATCH));
      assertEquals(2, handler.calls.get()); // backpressure once, admitted on retry
      closeQuietly(client);
    } finally {
      server.shutdownNow();
    }
  }

  /**
   * A checkpoint-continuity gap (FAILED_PRECONDITION, task-194) is <b>not</b> retryable: the batch
   * doesn't continue from the shard's checkpoint, so retrying the same batch can't help — it must
   * propagate so the connector fails the trigger (and the discontinuity is resolved out of band).
   */
  @Test
  void writeDoesNotRetryACheckpointGap() throws IOException {
    FlakyWrite handler =
        new FlakyWrite(/* failFirst= */ 1, /* snapshot= */ 0, Status.FAILED_PRECONDITION);
    Server server = ServerBuilder.forPort(0).addService(handler).build().start();
    try {
      WriteClient client = new WriteClient("127.0.0.1", server.getPort(), 5, 4, 20, 40);
      StatusRuntimeException e =
          assertThrows(StatusRuntimeException.class, () -> client.write(BATCH));
      assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
      assertEquals(1, handler.calls.get()); // tried once, no retry
      closeQuietly(client);
    } finally {
      server.shutdownNow();
    }
  }

  /** An application error (not a transport failure) propagates immediately, with no retry. */
  @Test
  void writeDoesNotRetryAnApplicationError() throws IOException {
    AlwaysRejects handler = new AlwaysRejects();
    Server server = ServerBuilder.forPort(0).addService(handler).build().start();
    try {
      WriteClient client = new WriteClient("127.0.0.1", server.getPort(), 5, 4, 20, 40);
      StatusRuntimeException e =
          assertThrows(StatusRuntimeException.class, () -> client.write(BATCH));
      assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
      assertEquals(1, handler.calls.get()); // tried once, no retry
      closeQuietly(client);
    } finally {
      server.shutdownNow();
    }
  }

  private static void closeQuietly(WriteClient client) {
    try {
      client.close();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Never answers — models a Node whose network was torn down (force-killed pod). */
  private static final class HangingWrite extends WriteGrpc.WriteImplBase {
    @Override
    public void write(WriteRequest request, StreamObserver<WriteResponse> obs) {
      // intentionally never onNext/onError — the client's deadline must cancel the call
    }
  }

  /** Fails the first {@code failFirst} calls with {@code failWith}, then commits {@code snapshot}. */
  private static final class FlakyWrite extends WriteGrpc.WriteImplBase {
    final AtomicInteger calls = new AtomicInteger();
    private final int failFirst;
    private final long snapshot;
    private final Status failWith;

    FlakyWrite(int failFirst, long snapshot) {
      this(failFirst, snapshot, Status.UNAVAILABLE);
    }

    FlakyWrite(int failFirst, long snapshot, Status failWith) {
      this.failFirst = failFirst;
      this.snapshot = snapshot;
      this.failWith = failWith;
    }

    @Override
    public void write(WriteRequest request, StreamObserver<WriteResponse> obs) {
      if (calls.incrementAndGet() <= failFirst) {
        obs.onError(failWith.withDescription("node restarting").asRuntimeException());
        return;
      }
      obs.onNext(WriteResponse.newBuilder().setSnapshot(snapshot).build());
      obs.onCompleted();
    }
  }

  /** Always rejects with a non-retryable application error. */
  private static final class AlwaysRejects extends WriteGrpc.WriteImplBase {
    final AtomicInteger calls = new AtomicInteger();

    @Override
    public void write(WriteRequest request, StreamObserver<WriteResponse> obs) {
      calls.incrementAndGet();
      obs.onError(Status.INVALID_ARGUMENT.withDescription("bad batch").asRuntimeException());
    }
  }
}
