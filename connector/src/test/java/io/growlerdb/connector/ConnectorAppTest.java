package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.growlerdb.proto.v1.GetIndexResponse;
import io.growlerdb.proto.v1.RoutingStrategy;
import io.growlerdb.proto.v1.ShardStatus;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConnectorAppTest {

  @Test
  void s3CatalogConfMapsSetVarsUnderTheCatalogAndTrims() {
    Map<String, String> env = new HashMap<>();
    env.put("GROWLERDB_S3_ACCESS_KEY", "AKIA");
    env.put("GROWLERDB_S3_SECRET_KEY", "secret");
    env.put("GROWLERDB_S3_REGION", "  us-west-2 ");
    Map<String, String> conf = ConnectorApp.s3CatalogConf("stream", env);
    assertEquals("AKIA", conf.get("spark.sql.catalog.stream.s3.access-key-id"));
    assertEquals("secret", conf.get("spark.sql.catalog.stream.s3.secret-access-key"));
    assertEquals("us-west-2", conf.get("spark.sql.catalog.stream.client.region"));
    assertEquals(3, conf.size());
  }

  @Test
  void s3CatalogConfOmitsBlankOrUnsetSoTheAwsDefaultChainIsUsed() {
    Map<String, String> env = new HashMap<>();
    env.put("GROWLERDB_S3_ACCESS_KEY", "");
    // secret + region unset entirely; access-key blank — all omitted so S3FileIO uses the AWS chain.
    assertTrue(ConnectorApp.s3CatalogConf("stream", env).isEmpty());
  }

  private static GetIndexResponse.Builder indexWithShards(int count) {
    return GetIndexResponse.newBuilder().setShardCount(count).setRouting(RoutingStrategy.ROUTING_HASH);
  }

  @Test
  void shardEndpointsFromCpAreOrdinalOrderedAndSchemeStripped() {
    // Registry shard map delivered out of order + with a scheme prefix — the connector sorts by
    // ordinal and dials host:port, so writes go to the same nodes reads route to.
    GetIndexResponse entry =
        indexWithShards(3)
            .addShardStatus(ShardStatus.newBuilder().setOrdinal(2).setPrimary("http://n2:50051"))
            .addShardStatus(ShardStatus.newBuilder().setOrdinal(0).setPrimary("n0:50051"))
            .addShardStatus(ShardStatus.newBuilder().setOrdinal(1).setPrimary("https://n1:50051"))
            .build();
    assertEquals(
        List.of("n0:50051", "n1:50051", "n2:50051"), ConnectorApp.shardEndpointsFromCp(entry));
  }

  @Test
  void shardEndpointsFromCpFailsFastWhenAShardHasNoPrimaryYet() {
    // Shard 1's serve node hasn't registered → no primary. Fail loudly rather than silently drop
    // that shard's writes (which the min-checkpoint resume would then never advance past).
    GetIndexResponse entry =
        indexWithShards(2)
            .addShardStatus(ShardStatus.newBuilder().setOrdinal(0).setPrimary("n0:50051"))
            .addShardStatus(ShardStatus.newBuilder().setOrdinal(1).setPrimary(""))
            .build();
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> ConnectorApp.shardEndpointsFromCp(entry));
    assertTrue(e.getMessage().contains("shard 1"), e.getMessage());
  }

  @Test
  void shardEndpointsFromCpRejectsADuplicateOrdinal() {
    // One ShardStatus per ordinal is the registry contract; two entries claiming shard 1 make write
    // ownership ambiguous, so it must fail loudly rather than silently pick one.
    GetIndexResponse entry =
        indexWithShards(2)
            .addShardStatus(ShardStatus.newBuilder().setOrdinal(0).setPrimary("n0:50051"))
            .addShardStatus(ShardStatus.newBuilder().setOrdinal(1).setPrimary("n1:50051"))
            .addShardStatus(ShardStatus.newBuilder().setOrdinal(1).setPrimary("n9:50051"))
            .build();
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> ConnectorApp.shardEndpointsFromCp(entry));
    assertTrue(e.getMessage().contains("shard 1"), e.getMessage());
    assertTrue(e.getMessage().contains("n1:50051") && e.getMessage().contains("n9:50051"), e.getMessage());
  }

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
