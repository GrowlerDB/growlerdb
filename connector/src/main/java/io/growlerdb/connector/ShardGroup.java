package io.growlerdb.connector;

import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Deterministic shard-group assignment for the parallel connector set: worker
 * {@code i} of {@code W} owns the shards {@code {s : s % W == i}}. Every shard has exactly one
 * owner, so the Node's per-shard continuity guard needs no writer identity — a worker's
 * checkpoint namespace <i>is</i> its group. Pure math: the same (workerId, workers, shards)
 * always yields the same group, on every pod, with no coordination.
 */
final class ShardGroup {

  private ShardGroup() {}

  /**
   * The shard ordinals worker {@code workerId} of {@code workers} owns over {@code shards}
   * shards. Empty when {@code workers > shards} leaves this worker nothing — the caller must
   * fail fast (a silently idle pod hides a misconfiguration).
   */
  static SortedSet<Integer> owned(int workerId, int workers, int shards) {
    if (workers < 1) {
      throw new IllegalArgumentException("workers must be >= 1, got " + workers);
    }
    if (workerId < 0 || workerId >= workers) {
      throw new IllegalArgumentException(
          "worker-id must be in [0, " + workers + "), got " + workerId);
    }
    if (shards < 1) {
      throw new IllegalArgumentException("shards must be >= 1, got " + shards);
    }
    SortedSet<Integer> owned = new TreeSet<>();
    for (int s = workerId; s < shards; s += workers) {
      owned.add(s);
    }
    return owned;
  }
}
