package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * The concurrent per-shard fan-out (task-196 AC3, {@link ShardFanOut}): sub-batches of one batch
 * commit <b>overlapped</b> instead of via the old sequential blocking loop, every sub-batch —
 * including empties (task-194 lockstep) — is still sent, and a failure on one shard neither
 * prevents siblings from committing nor gets lost (all-settle, lowest-ordinal error propagates,
 * later ones suppressed). Against in-process Node stubs, no Spark needed.
 */
class ShardedWriteClientFanOutTest {

  private static final DocBatch EMPTY_BATCH = DocBatch.newBuilder().setBatchId("b1").build();

  /**
   * Concurrency proof: every server holds its response until ALL servers have received their
   * sub-batch. The old sequential loop deadlocks here (shard 0 blocks before shard 1 is ever
   * called); the overlapped fan-out sails through. An empty ops list means every sub-batch is
   * empty — passing also proves empties are still sent to every shard.
   */
  @Test
  void subBatchesCommitConcurrentlyNotSequentially() throws IOException {
    int n = 3;
    CountDownLatch allArrived = new CountDownLatch(n);
    List<Server> servers = new ArrayList<>(n);
    List<String> endpoints = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      Server s =
          ServerBuilder.forPort(0)
              .addService(new BarrierWrite(allArrived, /* snapshot= */ i + 1))
              .build()
              .start();
      servers.add(s);
      endpoints.add("127.0.0.1:" + s.getPort());
    }
    try {
      ShardedWriteClient client =
          new ShardedWriteClient(endpoints, ShardRouter.Strategy.HASH);
      double acksBefore = totalShardAcks(n);
      assertTimeoutPreemptively(
          Duration.ofSeconds(15),
          () -> assertEquals(n, client.write(EMPTY_BATCH), "max committed snapshot"));
      assertEquals(
          n, (int) (totalShardAcks(n) - acksBefore), "one ack recorded per shard, success only");
      closeQuietly(client);
    } finally {
      servers.forEach(Server::shutdownNow);
    }
  }

  /**
   * All-settle failure semantics: shard 0 gaps (FAILED_PRECONDITION), shard 1 rejects
   * (INVALID_ARGUMENT), shard 2 is healthy. The healthy shard still commits, the lowest-ordinal
   * failure is thrown, and the other failure rides along as suppressed — nothing is lost, and
   * there is no fail-fast that would strand a committable shard.
   */
  @Test
  void aFailedShardNeitherBlocksSiblingsNorGetsLost() throws IOException, InterruptedException {
    FailingWrite gap = new FailingWrite(Status.FAILED_PRECONDITION);
    FailingWrite reject = new FailingWrite(Status.INVALID_ARGUMENT);
    CountingWrite healthy = new CountingWrite(/* snapshot= */ 9);
    Server s0 = ServerBuilder.forPort(0).addService(gap).build().start();
    Server s1 = ServerBuilder.forPort(0).addService(reject).build().start();
    Server s2 = ServerBuilder.forPort(0).addService(healthy).build().start();
    try {
      ShardedWriteClient client =
          new ShardedWriteClient(
              List.of(
                  "127.0.0.1:" + s0.getPort(),
                  "127.0.0.1:" + s1.getPort(),
                  "127.0.0.1:" + s2.getPort()),
              ShardRouter.Strategy.HASH);
      StatusRuntimeException e =
          assertThrows(StatusRuntimeException.class, () -> client.write(EMPTY_BATCH));
      assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode(), "lowest ordinal wins");
      assertEquals(1, e.getSuppressed().length, "the sibling failure is attached, not lost");
      assertEquals(
          Status.Code.INVALID_ARGUMENT,
          ((StatusRuntimeException) e.getSuppressed()[0]).getStatus().getCode());
      assertTrue(
          waitFor(() -> healthy.calls.get() == 1),
          "the healthy shard still received and committed its sub-batch");
      assertEquals(1, gap.calls.get(), "FAILED_PRECONDITION stays non-retryable through the pool");
      closeQuietly(client);
    } finally {
      s0.shutdownNow();
      s1.shutdownNow();
      s2.shutdownNow();
    }
  }

  private static double totalShardAcks(int shards) {
    double total = 0;
    for (int i = 0; i < shards; i++) {
      total += ConnectorMetrics.SHARD_ACKS.labels(Integer.toString(i)).get();
    }
    return total;
  }

  private static boolean waitFor(java.util.function.BooleanSupplier condition)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return true;
      }
      Thread.sleep(10);
    }
    return condition.getAsBoolean();
  }

  private static void closeQuietly(ShardedWriteClient client) {
    try {
      client.close();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Counts down on arrival, then waits for every sibling before answering. */
  private static final class BarrierWrite extends WriteGrpc.WriteImplBase {
    private final CountDownLatch allArrived;
    private final long snapshot;

    BarrierWrite(CountDownLatch allArrived, long snapshot) {
      this.allArrived = allArrived;
      this.snapshot = snapshot;
    }

    @Override
    public void write(WriteRequest request, StreamObserver<WriteResponse> obs) {
      allArrived.countDown();
      try {
        if (!allArrived.await(10, TimeUnit.SECONDS)) {
          obs.onError(
              Status.DEADLINE_EXCEEDED
                  .withDescription("siblings never arrived — fan-out is sequential")
                  .asRuntimeException());
          return;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        obs.onError(Status.CANCELLED.asRuntimeException());
        return;
      }
      obs.onNext(WriteResponse.newBuilder().setSnapshot(snapshot).build());
      obs.onCompleted();
    }
  }

  /** Always fails with the given (non-retryable) status. */
  private static final class FailingWrite extends WriteGrpc.WriteImplBase {
    final AtomicInteger calls = new AtomicInteger();
    private final Status status;

    FailingWrite(Status status) {
      this.status = status;
    }

    @Override
    public void write(WriteRequest request, StreamObserver<WriteResponse> obs) {
      calls.incrementAndGet();
      obs.onError(status.withDescription("nope").asRuntimeException());
    }
  }

  /** Commits every batch at {@code snapshot} and counts calls. */
  private static final class CountingWrite extends WriteGrpc.WriteImplBase {
    final AtomicInteger calls = new AtomicInteger();
    private final long snapshot;

    CountingWrite(long snapshot) {
      this.snapshot = snapshot;
    }

    @Override
    public void write(WriteRequest request, StreamObserver<WriteResponse> obs) {
      calls.incrementAndGet();
      obs.onNext(WriteResponse.newBuilder().setSnapshot(snapshot).build());
      obs.onCompleted();
    }
  }
}
