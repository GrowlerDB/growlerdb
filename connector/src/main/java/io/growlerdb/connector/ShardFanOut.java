package io.growlerdb.connector;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Overlapped per-shard fan-out (task-196): runs one blocking per-shard write per pooled thread
 * and joins them <b>all</b> before returning, so a chunk's sub-batches commit concurrently
 * instead of serially — the slowest shard, not the sum of all shards, bounds a chunk's wall
 * clock. The join-all barrier is what keeps the continuity guard's per-shard ordering intact:
 * the next chunk is not fanned out until every shard has settled this one.
 *
 * <p>Failure semantics mirror the sequential loop it replaces, minus the ordering artifact:
 * <b>no fail-fast</b> — shards that can commit do commit (a failed sibling just lags and the
 * resume-from-min machinery covers it) — and after all writes settle, the lowest-ordinal
 * failure propagates with the others attached as suppressed. Retry/backoff and the
 * non-retryable set stay inside {@link WriteClient}, untouched.
 */
final class ShardFanOut {

  private final ExecutorService pool;

  /** One daemon thread per shard: each task is one blocking RPC (plus its retry loop). */
  ShardFanOut(int shards) {
    AtomicInteger seq = new AtomicInteger();
    this.pool =
        Executors.newFixedThreadPool(
            shards,
            r -> {
              Thread t = new Thread(r, "growlerdb-shard-writer-" + seq.getAndIncrement());
              t.setDaemon(true);
              return t;
            });
  }

  /**
   * Run every write concurrently, join them all, and return the max committed snapshot.
   * On failure, throws the lowest-index task's error (unwrapped, so a
   * {@link StatusRuntimeException} keeps its status) with later failures suppressed. An
   * interrupt while joining restores the flag and surfaces as {@code CANCELLED}, matching
   * {@link WriteClient}'s convention; in-flight writes are left to settle on their own threads.
   */
  long maxSnapshot(List<Callable<Long>> writes) {
    List<Future<Long>> futures = new ArrayList<>(writes.size());
    for (Callable<Long> write : writes) {
      futures.add(pool.submit(write));
    }
    long max = 0;
    RuntimeException failure = null;
    for (Future<Long> future : futures) {
      try {
        max = Math.max(max, future.get());
      } catch (ExecutionException e) {
        RuntimeException unwrapped = unwrap(e);
        if (failure == null) {
          failure = unwrapped;
        } else {
          failure.addSuppressed(unwrapped);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        StatusRuntimeException cancelled =
            new StatusRuntimeException(
                Status.CANCELLED.withDescription("shard fan-out interrupted"));
        if (failure != null) {
          cancelled.addSuppressed(failure);
        }
        throw cancelled;
      }
    }
    if (failure != null) {
      throw failure;
    }
    return max;
  }

  private static RuntimeException unwrap(ExecutionException e) {
    Throwable cause = e.getCause();
    if (cause instanceof RuntimeException runtime) {
      return runtime;
    }
    if (cause instanceof Error error) {
      throw error;
    }
    return new RuntimeException(cause);
  }

  void close() throws InterruptedException {
    pool.shutdown();
    pool.awaitTermination(5, TimeUnit.SECONDS);
  }
}
