package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.growlerdb.proto.v1.Coordinates;
import io.growlerdb.proto.v1.Field;
import io.growlerdb.proto.v1.Value;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The parallel connector set end-to-end in Spark local mode, against {@link
 * FakeShardNode}s that ENFORCE the window-covering continuity guard, dedup, and pruning:
 *
 * <ul>
 *   <li>two workers with skewed trigger heads partition one table with zero guard trips and
 *       exact {@link ShardRouter} placement;
 *   <li>regrouping (W=2 → W=1) is self-healing — plain resume-from-lineage-min, no alignment
 *       protocol, no coordination;
 *   <li>and it stays self-healing when every idempotency record has been PRUNED, proving the
 *       set depends on the covering guard, not on batch-id dedup.
 * </ul>
 */
@Tag("integration")
class ConnectorSetIntegrationTest {

  private static SparkSession spark;
  private static Path warehouse;
  private final List<Server> servers = new ArrayList<>();

  @BeforeAll
  static void startSpark() throws IOException {
    warehouse = Files.createTempDirectory("growlerdb-connector-set-it");
    spark =
        SparkSession.builder()
            .appName("growlerdb-connector-set-it")
            .master("local[2]")
            .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.demo.type", "hadoop")
            .config("spark.sql.catalog.demo.warehouse", warehouse.toString())
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.ui.enabled", "false")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @AfterEach
  void stopServers() {
    servers.forEach(Server::shutdownNow);
    servers.clear();
  }

  @Test
  void workersPartitionRegroupAndSurvivePruningWithoutGuardTrips() throws Exception {
    spark.sql("DROP TABLE IF EXISTS demo.ns.set");
    spark.sql(
        "CREATE TABLE demo.ns.set (id STRING, body STRING) USING iceberg "
            + "TBLPROPERTIES ('format-version'='2')");
    insertRows(0, 8);

    List<FakeShardNode> fakes = new ArrayList<>();
    List<String> endpoints = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      FakeShardNode fake = new FakeShardNode();
      Server server = ServerBuilder.forPort(0).addService(fake).build().start();
      fakes.add(fake);
      servers.add(server);
      endpoints.add("127.0.0.1:" + server.getPort());
    }
    ShardRouter router = new ShardRouter(3, ShardRouter.Strategy.HASH);
    SnapshotLineage lineage = SnapshotLineage.forTable(spark, "demo.ns.set");
    ConnectorJob base =
        new ConnectorJob(
            "demo",
            "ns.set",
            new IndexMapping(List.of(), List.of("id"), List.of("id", "body")),
            List.of("id"));

    // --- Phase 1: two workers, deliberately skewed trigger heads -------------------------
    SortedSet<Integer> g0 = ShardGroup.owned(0, 2, 3); // {0, 2}
    SortedSet<Integer> g1 = ShardGroup.owned(1, 2, 3); // {1}
    ConnectorJob w0 = base.ownedBy(router, g0);
    ConnectorJob w1 = base.ownedBy(router, g1);

    try (ShardGroupWriteClient c0 = new ShardGroupWriteClient(endpoints, router, lineage, g0);
        ShardGroupWriteClient c1 = new ShardGroupWriteClient(endpoints, router, lineage, g1)) {

      // Worker 0 runs at head H1; MORE rows land before worker 1 ever triggers (head skew).
      assertTrue(w0.runOnce(spark, c0.checkpointSnapshotId(), c0).wrote);
      insertRows(8, 4);
      assertTrue(w1.runOnce(spark, c1.checkpointSnapshotId(), c1).wrote);

      // Worker 1 saw all 12; worker 0's shards only the first 8 so far. Catch worker 0 up.
      assertTrue(w0.runOnce(spark, c0.checkpointSnapshotId(), c0).wrote);

      assertEquals(0, totalGaps(fakes), "no continuity-guard trips under skewed parallel ingest");
      assertPlacementMatches(fakes, router, 12);

      // Caught up ⇒ no-op, no RPCs.
      int rpcs = totalReceived(fakes);
      assertFalse(w0.runOnce(spark, c0.checkpointSnapshotId(), c0).wrote);
      assertFalse(w1.runOnce(spark, c1.checkpointSnapshotId(), c1).wrote);
      assertEquals(rpcs, totalReceived(fakes), "caught-up workers send nothing");
    }

    // --- Phase 2: regroup W=2 → W=1 with skewed shards ------------------------------------
    // Skew again: advance only worker 0's shards to a new head, leaving shard 1 behind.
    insertRows(12, 4);
    try (ShardGroupWriteClient c0 = new ShardGroupWriteClient(endpoints, router, lineage, g0)) {
      assertTrue(base.ownedBy(router, g0).runOnce(spark, c0.checkpointSnapshotId(), c0).wrote);
    }
    // One worker takes over everything: plain resume from the lineage-min across ITS new group.
    SortedSet<Integer> all = new TreeSet<>(Set.of(0, 1, 2));
    try (ShardGroupWriteClient cAll = new ShardGroupWriteClient(endpoints, router, lineage, all)) {
      assertTrue(base.ownedBy(router, all).runOnce(spark, cAll.checkpointSnapshotId(), cAll).wrote);
      assertEquals(0, totalGaps(fakes), "regrouping is self-healing (covering guard)");
      assertPlacementMatches(fakes, router, 16);
      assertTrue(cAll.drainedTo(base.currentSnapshotId(spark)), "all shards converged on the head");
    }

    // --- Phase 3: regroup with EVERY idempotency record pruned ----------------------------
    // The set must not lean on batch-id dedup for ahead shards: wipe the fakes' batch keys
    // (max-aggression prune), skew, regroup again.
    insertRows(16, 4);
    try (ShardGroupWriteClient c0 = new ShardGroupWriteClient(endpoints, router, lineage, g0)) {
      assertTrue(base.ownedBy(router, g0).runOnce(spark, c0.checkpointSnapshotId(), c0).wrote);
    }
    fakes.forEach(f -> f.batchKeys.clear());
    try (ShardGroupWriteClient cAll = new ShardGroupWriteClient(endpoints, router, lineage, all)) {
      assertTrue(base.ownedBy(router, all).runOnce(spark, cAll.checkpointSnapshotId(), cAll).wrote);
      assertEquals(0, totalGaps(fakes), "pruned dedup records cannot wedge a regroup");
      assertPlacementMatches(fakes, router, 20);
      assertTrue(cAll.drainedTo(base.currentSnapshotId(spark)));
    }
  }

  /** Insert rows k{from}..k{from+count-1} in one snapshot. */
  private static void insertRows(int from, int count) {
    String values =
        IntStream.range(from, from + count)
            .mapToObj(i -> "('k" + i + "','b" + i + "')")
            .collect(Collectors.joining(", "));
    spark.sql("INSERT INTO demo.ns.set VALUES " + values);
  }

  /** Every fake shard holds exactly the keys the router places on it — no loss, no bleed. */
  private static void assertPlacementMatches(
      List<FakeShardNode> fakes, ShardRouter router, int totalKeys) {
    for (int shard = 0; shard < fakes.size(); shard++) {
      Set<String> expected = new TreeSet<>();
      for (int k = 0; k < totalKeys; k++) {
        String id = "k" + k;
        if (router.route(keyOf(id)) == shard) {
          expected.add(id);
        }
      }
      assertEquals(
          expected,
          new TreeSet<>(fakes.get(shard).applied.keySet()),
          "shard " + shard + " holds exactly its routed keys");
    }
  }

  private static Coordinates keyOf(String id) {
    return Coordinates.newBuilder()
        .addIdentifier(
            Field.newBuilder().setName("id").setValue(Value.newBuilder().setStr(id).build()))
        .build();
  }

  private static int totalGaps(List<FakeShardNode> fakes) {
    return fakes.stream().mapToInt(f -> f.gaps.get()).sum();
  }

  private static int totalReceived(List<FakeShardNode> fakes) {
    return fakes.stream().mapToInt(f -> f.received.size()).sum();
  }
}
