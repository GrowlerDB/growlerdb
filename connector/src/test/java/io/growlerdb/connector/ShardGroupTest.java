package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/** The shard-group assignment math: deterministic, total, disjoint. */
class ShardGroupTest {

  @Test
  void groupsPartitionTheShardsDisjointlyAndTotally() {
    int shards = 7;
    int workers = 3;
    SortedSet<Integer> union = new TreeSet<>();
    int total = 0;
    for (int w = 0; w < workers; w++) {
      SortedSet<Integer> owned = ShardGroup.owned(w, workers, shards);
      total += owned.size();
      union.addAll(owned);
    }
    assertEquals(shards, total, "no shard owned twice");
    assertEquals(shards, union.size(), "no shard unowned");
    assertEquals(Set.of(0, 3, 6), ShardGroup.owned(0, 3, 7));
    assertEquals(Set.of(1, 4), ShardGroup.owned(1, 3, 7));
    assertEquals(Set.of(2, 5), ShardGroup.owned(2, 3, 7));
  }

  @Test
  void singleWorkerOwnsEverythingAndExtrasOwnNothing() {
    assertEquals(Set.of(0, 1, 2), ShardGroup.owned(0, 1, 3), "W=1 = the classic connector");
    assertTrue(ShardGroup.owned(4, 6, 3).isEmpty(), "worker beyond the shard count owns nothing");
  }

  @Test
  void invalidShapesFailFast() {
    assertThrows(IllegalArgumentException.class, () -> ShardGroup.owned(0, 0, 3));
    assertThrows(IllegalArgumentException.class, () -> ShardGroup.owned(-1, 2, 3));
    assertThrows(IllegalArgumentException.class, () -> ShardGroup.owned(2, 2, 3));
    assertThrows(IllegalArgumentException.class, () -> ShardGroup.owned(0, 1, 0));
  }
}
