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
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Cross-process integration: the JVM connector writes, over the
 * <b>real Write gRPC</b>, to a GrowlerDB Node running as a separate process (the Rust
 * {@code growlerdb serve} binary), and the committed docs are then <b>searchable</b> via
 * {@code growlerdb search} — proving the JVM↔Rust boundary end-to-end (mirrors the Rust
 * {@code growlerdb_serve_hosts_write_grpc} round-trip, across the language boundary).
 *
 * <p>{@code @Tag("e2e")} — excluded from {@code mvn verify}. Run with the binary
 * built (the {@code just connector-e2e} recipe builds it): {@code mvn test
 * -Dtest.excludedGroups= -Dgroups=e2e -DGROWLERDB_BIN=<path>}. Skips (does not fail) when
 * the binary is absent, with a logged reason — no silent pass.
 */
@Tag("e2e")
class ConnectorCrossProcessTest {

  @Test
  void jvmConnectorWritesToGrowlerDBServeAndIsSearchable() throws Exception {
    Path growlerdbBin = locateGrowlerDBBinary();
    assumeTrue(
        growlerdbBin != null && Files.isExecutable(growlerdbBin),
        "growlerdb binary not found — set -DGROWLERDB_BIN=<path> or run `cargo build` (skipping e2e)");

    Path dataDir = Files.createTempDirectory("growlerdb-connector-e2e");
    Path warehouse = Files.createTempDirectory("growlerdb-connector-e2e-wh");

    // Define the `docs` index on disk (the fixture is a serialized Rust ResolvedIndex)
    // so `growlerdb serve` can open its shard.
    Files.createDirectories(dataDir.resolve("docs"));
    Files.write(
        dataDir.resolve("docs/index.json"),
        getClass().getResourceAsStream("/growlerdb-docs-index.json").readAllBytes());

    int port = freePort();
    ConnectorJob job =
        new ConnectorJob(
            "demo",
            "ns.docs",
            new IndexMapping(List.of(), List.of("id"), List.of("id", "body")),
            List.of("id"));

    SparkSession spark = null;
    Long checkpoint;
    try {
      spark = sparkLocal(warehouse);
      spark.sql("CREATE TABLE demo.ns.docs (id STRING, body STRING) USING iceberg");
      spark.sql("INSERT INTO demo.ns.docs VALUES ('doc-1','hello'), ('doc-2','world')");
      spark.sql("UPDATE demo.ns.docs SET body = 'updated' WHERE id = 'doc-1'");
      spark.sql("DELETE FROM demo.ns.docs WHERE id = 'doc-2'");

      // First run: empty Node, no checkpoint yet → read from the start.
      Process server = spawnGrowlerDBServe(growlerdbBin, dataDir, port);
      try (WriteClient client = new WriteClient("127.0.0.1", port)) {
        assertEquals(null, client.checkpointSnapshotId(), "no checkpoint before first commit");
        ConnectorJob.Result r = job.runOnce(spark, client.checkpointSnapshotId(), client);
        assertTrue(r.wrote, "connector should commit a batch to growlerdb serve");
        assertEquals(1L, r.committedSnapshot, "first commit → index snapshot 1");
        checkpoint = r.checkpointSnapshotId;
        assertEquals(checkpoint, client.checkpointSnapshotId(), "checkpoint now readable over gRPC");
      } finally {
        stop(server);
      }

      // Restart the Node, then resume: the checkpoint survived, and replaying
      // the same window is a no-op (exactly-once after a connector/Node restart).
      Process restarted = spawnGrowlerDBServe(growlerdbBin, dataDir, port);
      try (WriteClient client = new WriteClient("127.0.0.1", port)) {
        assertEquals(
            checkpoint, client.checkpointSnapshotId(), "checkpoint durable across Node restart");
        ConnectorJob.Result resumed = job.runOnce(spark, client.checkpointSnapshotId(), client);
        assertFalse(resumed.wrote, "resuming from the committed checkpoint → no new work");
      } finally {
        stop(restarted);
      }
    } finally {
      if (spark != null) {
        spark.stop();
      }
    }

    // Node stopped (store lock released) → confirm the data persisted and the
    // updated doc is searchable, while the deleted one is gone.
    String hits = runGrowlerDB(growlerdbBin, dataDir, "body:updated");
    assertTrue(hits.contains("doc-1"), "doc-1 (updated) should be searchable; got:\n" + hits);
    String gone = runGrowlerDB(growlerdbBin, dataDir, "body:world");
    assertTrue(!gone.contains("doc-2"), "doc-2 was deleted; should not match; got:\n" + gone);
  }

  /** Spawn `growlerdb serve` on {@code port} and wait until it accepts connections. */
  private static Process spawnGrowlerDBServe(Path bin, Path dataDir, int port) throws Exception {
    Process p =
        new ProcessBuilder(
                bin.toString(),
                "--data-dir",
                dataDir.toString(),
                "serve",
                "docs",
                "--addr",
                "127.0.0.1:" + port)
            .inheritIO()
            .start();
    awaitPort(port, Duration.ofSeconds(20));
    return p;
  }

  /** Stop the server and wait for the OS to release its store lock / port. */
  private static void stop(Process server) throws InterruptedException {
    server.destroy();
    server.waitFor();
    Thread.sleep(200);
  }

  /** Run `growlerdb search docs "<query>"` and return its stdout. */
  private static String runGrowlerDB(Path bin, Path dataDir, String query) throws Exception {
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
        .appName("growlerdb-connector-e2e")
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

  /** {@code -DGROWLERDB_BIN}, else the workspace's {@code target/debug/growlerdb}. */
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

  /** Poll until something accepts on {@code port} (growlerdb serve is up) or time out. */
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
