package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.growlerdb.proto.v1.RoutingStrategy;
import java.util.List;
import org.junit.jupiter.api.Test;

class ConnectorAppTest {

  @Test
  void routingFollowsPartitionFields() {
    // Partitioned key → partition routing; unpartitioned → hash. Matches the Gateway's
    // ResolvedIndex::routing_strategy so writes land where reads look.
    assertEquals(ShardRouter.Strategy.HASH, ConnectorApp.routingFor(List.of()));
    assertEquals(ShardRouter.Strategy.PARTITION, ConnectorApp.routingFor(List.of("day")));
    assertEquals(
        ShardRouter.Strategy.PARTITION, ConnectorApp.routingFor(List.of("region", "tier")));
  }

  @Test
  void strategyOfMapsTheWireEnum() {
    assertEquals(ShardRouter.Strategy.HASH, ConnectorApp.strategyOf(RoutingStrategy.ROUTING_HASH));
    assertEquals(
        ShardRouter.Strategy.PARTITION,
        ConnectorApp.strategyOf(RoutingStrategy.ROUTING_PARTITION));
  }

  @Test
  void resolveRoutingAcceptsAMatchingConfig() {
    // 2 endpoints, registry says 2 shards + hash, no --partition → hash. Agrees.
    assertEquals(
        ShardRouter.Strategy.HASH,
        ConnectorApp.resolveRouting(2, ShardRouter.Strategy.HASH, 2, List.of()));
    // Partitioned: registry partition + --partition set → partition.
    assertEquals(
        ShardRouter.Strategy.PARTITION,
        ConnectorApp.resolveRouting(3, ShardRouter.Strategy.PARTITION, 3, List.of("region")));
  }

  @Test
  void resolveRoutingFailsFastOnShardCountMismatch() {
    // 4 endpoints but the registry has 8 shards — writes by %4, reads by %8.
    assertThrows(
        IllegalStateException.class,
        () -> ConnectorApp.resolveRouting(8, ShardRouter.Strategy.HASH, 4, List.of()));
  }

  @Test
  void resolveRoutingFailsFastOnStrategyMismatch() {
    // Registry resolves the index to partition routing, but --partition is empty (→ hash).
    assertThrows(
        IllegalStateException.class,
        () -> ConnectorApp.resolveRouting(2, ShardRouter.Strategy.PARTITION, 2, List.of()));
  }
}
