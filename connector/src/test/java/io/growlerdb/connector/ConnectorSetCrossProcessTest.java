package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The parallel connector set against REAL {@code growlerdb serve} shards (task-196): two workers,
 * each owning one of two sharded Nodes (`--shards 2 --shard-ordinal k`), ingest one table in
 * parallel over the real Write gRPC — per-shard continuity guard, sequence-stamped checkpoints,
 * dedup and pruning all live. Covers: parallel commit with skewed worker triggers, checkpoint
 * durability across a Node restart, no-op resume, and disjoint per-shard search results.
 *
 * <p>{@code @Tag("e2e")} — run with {@code mvn test -Dtest.excludedGroups= -Dgroups=e2e}; skips
 * when the binary is absent.
 */
@Tag("e2e")
class ConnectorSetCrossProcessTest {

  @Test
  void twoWorkersIngestOneTableIntoTwoRealShards() throws Exception {
    Path bin = locateGrowlerDBBinary();
    assumeTrue(
        bin != null && Files.isExecutable(bin),
        "growlerdb binary not found — set -DGROWLERDB_BIN=<path> or run `cargo build` (skipping e2e)");

    Path warehouse = Files.createTempDirectory("growlerdb-set-e2e-wh");
    Path[] dataDirs = {
      Files.createTempDirectory("growlerdb-set-e2e-s0"), Files.createTempDirectory("growlerdb-set-e2e-s1")
    };
    byte[] indexFixture = getClass().getResourceAsStream("/growlerdb-docs-index.json").readAllBytes();
    for (Path dir : dataDirs) {
      Files.createDirectories(dir.resolve("docs"));
      Files.write(dir.resolve("docs/index.json"), indexFixture);
    }
    int[] ports = {freePort(), freePort()};
    List<String> endpoints = List.of("127.0.0.1:" + ports[0], "127.0.0.1:" + ports[1]);
    ShardRouter router = new ShardRouter(2, ShardRouter.Strategy.HASH);
    SortedSet<Integer> g0 = new TreeSet<>(List.of(0));
    SortedSet<Integer> g1 = new TreeSet<>(List.of(1));

    SparkSession spark = null;
    Process[] shards = {null, null};
    try {
      spark = sparkLocal(warehouse);
      spark.sql(
          "CREATE TABLE demo.ns.docs (id STRING, body STRING) USING iceberg "
              + "TBLPROPERTIES ('format-version'='2')");
      insertRows(spark, 0, 6);

      shards[0] = spawnServe(bin, dataDirs[0], 2, 0, ports[0]);
      shards[1] = spawnServe(bin, dataDirs[1], 2, 1, ports[1]);

      ConnectorJob base =
          new ConnectorJob(
              "demo",
              "ns.docs",
              new IndexMapping(List.of(), List.of("id"), List.of("id", "body")),
              List.of("id"));
      SnapshotLineage lineage = SnapshotLineage.forTable(spark, "demo.ns.docs");
      ConnectorJob w0 = base.ownedBy(router, g0);
      ConnectorJob w1 = base.ownedBy(router, g1);

      Long head;
      try (ShardGroupWriteClient c0 = new ShardGroupWriteClient(endpoints, router, lineage, g0);
          ShardGroupWriteClient c1 = new ShardGroupWriteClient(endpoints, router, lineage, g1)) {
        // Parallel ingest with skewed triggers: worker 0 commits at head H1; more rows land;
        // worker 1 first triggers at H2. Two writers, one table, zero guard trips.
        assertTrue(w0.runOnce(spark, c0.checkpointSnapshotId(), c0).wrote, "worker 0 commits");
        insertRows(spark, 6, 2);
        assertTrue(w1.runOnce(spark, c1.checkpointSnapshotId(), c1).wrote, "worker 1 commits");
        assertTrue(w0.runOnce(spark, c0.checkpointSnapshotId(), c0).wrote, "worker 0 catches up");

        head = base.currentSnapshotId(spark);
        assertTrue(c0.drainedTo(head), "shard 0 converged on the head");
        assertTrue(c1.drainedTo(head), "shard 1 converged on the head");
      }

      // Node restart: shard 0's checkpoint is durable and the worker's resume is a no-op.
      stop(shards[0]);
      shards[0] = spawnServe(bin, dataDirs[0], 2, 0, ports[0]);
      try (ShardGroupWriteClient c0 = new ShardGroupWriteClient(endpoints, router, lineage, g0)) {
        assertEquals(head, c0.checkpointSnapshotId(), "checkpoint durable across the Node restart");
        assertFalse(w0.runOnce(spark, c0.checkpointSnapshotId(), c0).wrote, "resume is a no-op");
      }
    } finally {
      for (Process shard : shards) {
        if (shard != null) {
          stop(shard);
        }
      }
      if (spark != null) {
        spark.stop();
      }
    }

    // Each shard serves exactly its routed keys — disjoint, and together all eight.
    SortedSet<String> all = new TreeSet<>();
    for (int shard = 0; shard < 2; shard++) {
      String hits = search(bin, dataDirs[shard], "body:hello");
      SortedSet<String> expected = new TreeSet<>();
      for (int k = 0; k < 8; k++) {
        String id = "k" + k;
        if (router.route(keyOf(id)) == shard) {
          expected.add(id);
        }
      }
      for (String id : expected) {
        assertTrue(hits.contains(id), "shard " + shard + " should serve " + id + "; got:\n" + hits);
      }
      all.addAll(expected);
    }
    assertEquals(8, all.size(), "the two shards together cover every key");
  }

  private static void insertRows(SparkSession spark, int from, int count) {
    String values =
        IntStream.range(from, from + count)
            .mapToObj(i -> "('k" + i + "','hello b" + i + "')")
            .collect(Collectors.joining(", "));
    spark.sql("INSERT INTO demo.ns.docs VALUES " + values);
  }

  private static io.growlerdb.proto.v1.Coordinates keyOf(String id) {
    return io.growlerdb.proto.v1.Coordinates.newBuilder()
        .addIdentifier(
            io.growlerdb.proto.v1.Field.newBuilder()
                .setName("id")
                .setValue(io.growlerdb.proto.v1.Value.newBuilder().setStr(id).build()))
        .build();
  }

  private static Process spawnServe(Path bin, Path dataDir, int shards, int ordinal, int port)
      throws Exception {
    Process p =
        new ProcessBuilder(
                bin.toString(),
                "--data-dir",
                dataDir.toString(),
                "serve",
                "docs",
                "--shards",
                Integer.toString(shards),
                "--shard-ordinal",
                Integer.toString(ordinal),
                "--addr",
                "127.0.0.1:" + port)
            .inheritIO()
            .start();
    awaitPort(port, Duration.ofSeconds(20));
    return p;
  }

  private static void stop(Process server) throws InterruptedException {
    server.destroy();
    server.waitFor();
    Thread.sleep(200);
  }

  private static String search(Path bin, Path dataDir, String query) throws Exception {
    Process p =
        new ProcessBuilder(
                bin.toString(), "--data-dir", dataDir.toString(), "search", "docs", query)
            .redirectErrorStream(false)
            .start();
    String out = new String(p.getInputStream().readAllBytes());
    p.waitFor();
    return out;
  }

  private static SparkSession sparkLocal(Path warehouse) {
    return SparkSession.builder()
        .appName("growlerdb-set-e2e")
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

  private static Path locateGrowlerDBBinary() {
    String prop = System.getProperty("GROWLERDB_BIN", System.getenv("GROWLERDB_BIN"));
    if (prop != null && !prop.isBlank()) {
      return Paths.get(prop);
    }
    Path guess = Paths.get("..", "target", "debug", "growlerdb").toAbsolutePath().normalize();
    return Files.exists(guess) ? guess : null;
  }

  private static int freePort() throws IOException {
    try (ServerSocket s = new ServerSocket(0)) {
      return s.getLocalPort();
    }
  }

  private static void awaitPort(int port, Duration timeout) throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      try (java.net.Socket s = new java.net.Socket("127.0.0.1", port)) {
        return;
      } catch (IOException retry) {
        Thread.sleep(50);
      }
    }
    throw new IllegalStateException("growlerdb serve did not come up on port " + port);
  }
}
