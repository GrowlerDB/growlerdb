package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.growlerdb.proto.v1.DocBatch;
import io.growlerdb.proto.v1.DocOp;
import io.growlerdb.proto.v1.WriteGrpc;
import io.growlerdb.proto.v1.WriteRequest;
import io.growlerdb.proto.v1.WriteResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * End-to-end of the connector pipeline in Spark <b>local mode</b>: a temp
 * Iceberg table (Hadoop catalog) gets INSERT/UPDATE/DELETE, {@link ConnectorJob}
 * reads the changelog → maps → commits over the real Write gRPC to an in-process
 * Node stub, and we assert the committed {@link DocBatch}. This proves the
 * read→map→write wiring across the gRPC boundary without a separate process; the
 * cross-process variant against the real {@code growlerdb serve} binary is {@link
 * ConnectorCrossProcessTest}.
 *
 * <p>{@code @Tag("integration")} — heavy (pulls the Spark/Iceberg runtime), excluded
 * from the default {@code mvn verify}; run with {@code mvn test -Dgroups=integration
 * -Dtest.excludedGroups=}.
 */
@Tag("integration")
class ConnectorJobIntegrationTest {

  private static SparkSession spark;
  private static Path warehouse;

  @BeforeAll
  static void startSpark() throws IOException {
    warehouse = Files.createTempDirectory("growlerdb-connector-it");
    spark =
        SparkSession.builder()
            .appName("growlerdb-connector-it")
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

  @Test
  void readsChangelogAndCommitsOverGrpc() throws Exception {
    spark.sql("DROP TABLE IF EXISTS demo.ns.docs");
    spark.sql("CREATE TABLE demo.ns.docs (id STRING, body STRING) USING iceberg");
    spark.sql("INSERT INTO demo.ns.docs VALUES ('doc-1','hello'), ('doc-2','world')");
    spark.sql("UPDATE demo.ns.docs SET body = 'updated' WHERE id = 'doc-1'");
    spark.sql("DELETE FROM demo.ns.docs WHERE id = 'doc-2'");

    RecordingWrite node = new RecordingWrite();
    Server server = ServerBuilder.forPort(0).addService(node).build().start();
    try {
      ConnectorJob job =
          new ConnectorJob(
              "demo",
              "ns.docs",
              new IndexMapping(List.of(), List.of("id"), List.of("id", "body")),
              List.of("id"));

      try (WriteClient client = new WriteClient("127.0.0.1", server.getPort())) {
        // From the start (null checkpoint) → the whole history collapses to the
        // net effect: doc-1 present (updated), doc-2 gone.
        ConnectorJob.Result r = job.runOnce(spark, null, client);
        assertTrue(r.wrote, "a batch should be committed");
        assertEquals(1L, r.committedSnapshot, "first commit → index snapshot 1");
        assertEquals(r.checkpointSnapshotId, job.currentSnapshotId(spark));

        // Idempotent resume: re-running from the same checkpoint is a no-op.
        ConnectorJob.Result again = job.runOnce(spark, r.checkpointSnapshotId, client);
        assertFalse(again.wrote, "already caught up → no RPC");
      }

      assertEquals(1, node.received.size(), "exactly one batch sent");
      DocBatch batch = node.received.get(0);
      Map<String, DocOp> byKey = byIdentifier(batch);

      DocOp doc1 = byKey.get("doc-1");
      assertTrue(doc1.hasUpsert(), "doc-1 → upsert");
      assertEquals("updated", doc1.getUpsert().getDoc().getFieldsMap().get("body").getStr());

      DocOp doc2 = byKey.get("doc-2");
      assertTrue(doc2.hasDelete(), "doc-2 → delete");
    } finally {
      server.shutdownNow();
    }
  }

  @Test
  void boundedCatchUpSplitsALargeWindowAndStaysExactlyOnce() throws Exception {
    spark.sql("DROP TABLE IF EXISTS demo.ns.big");
    spark.sql("CREATE TABLE demo.ns.big (id STRING, body STRING) USING iceberg");
    // Three snapshots, two rows each (6 changelog rows). With a 2-row cap the window can't ride one
    // Write — it must split at snapshot boundaries into multiple bounded commits.
    spark.sql("INSERT INTO demo.ns.big VALUES ('a','1'), ('b','1')");
    spark.sql("INSERT INTO demo.ns.big VALUES ('c','1'), ('d','1')");
    spark.sql("INSERT INTO demo.ns.big VALUES ('e','1'), ('f','1')");

    RecordingWrite node = new RecordingWrite();
    Server server = ServerBuilder.forPort(0).addService(node).build().start();
    try {
      ConnectorJob job =
          new ConnectorJob(
              "demo",
              "ns.big",
              new IndexMapping(List.of(), List.of("id"), List.of("id", "body")),
              List.of("id"),
              java.util.Set.of(),
              2); // cap: 2 changelog rows per commit
      try (WriteClient client = new WriteClient("127.0.0.1", server.getPort())) {
        ConnectorJob.Result r = job.runOnce(spark, null, client);
        assertTrue(r.wrote, "a batch should be committed");
        assertEquals(job.currentSnapshotId(spark), r.checkpointSnapshotId, "advances to the head");

        // Bounded: the window committed as several batches, none oversized.
        assertTrue(node.received.size() >= 2, "large window split into multiple bounded commits");
        for (DocBatch b : node.received) {
          assertTrue(b.getOpsCount() <= 4, "each commit stays bounded (cap + one snapshot)");
        }

        // Exactly-once: every row applied once across the batches (no loss, no dup) and a resume
        // from the head is a no-op.
        Map<String, DocOp> all = new java.util.HashMap<>();
        int ops = 0;
        for (DocBatch b : node.received) {
          ops += b.getOpsCount();
          all.putAll(byIdentifier(b));
        }
        assertEquals(6, ops, "all six rows committed, none duplicated across commits");
        assertEquals(java.util.Set.of("a", "b", "c", "d", "e", "f"), all.keySet());

        assertFalse(job.runOnce(spark, r.checkpointSnapshotId, client).wrote, "caught up → no RPC");
      }
    } finally {
      server.shutdownNow();
    }
  }

  @Test
  void aRecreatedSourceFailsWithSourceRecreatedNotACrypticAncestorCrash() throws Exception {
    spark.sql("DROP TABLE IF EXISTS demo.ns.recreated");
    spark.sql("CREATE TABLE demo.ns.recreated (id STRING, body STRING) USING iceberg");
    spark.sql("INSERT INTO demo.ns.recreated VALUES ('a', '1')");

    RecordingWrite node = new RecordingWrite();
    Server server = ServerBuilder.forPort(0).addService(node).build().start();
    try {
      ConnectorJob job =
          new ConnectorJob(
              "demo",
              "ns.recreated",
              new IndexMapping(List.of(), List.of("id"), List.of("id", "body")),
              List.of("id"));
      try (WriteClient client = new WriteClient("127.0.0.1", server.getPort())) {
        // Build to a checkpoint.
        Long checkpoint = job.runOnce(spark, null, client).checkpointSnapshotId;
        assertTrue(checkpoint != null, "first run establishes a checkpoint");

        // Drop + recreate the source with the same name → a brand-new lineage; the old checkpoint
        // is no longer an ancestor of the head.
        spark.sql("DROP TABLE demo.ns.recreated");
        spark.sql("CREATE TABLE demo.ns.recreated (id STRING, body STRING) USING iceberg");
        spark.sql("INSERT INTO demo.ns.recreated VALUES ('b', '2')");

        // Resuming from the stale checkpoint is a clear SOURCE_RECREATED error — not Iceberg's
        // cryptic "not a parent ancestor" assertion — and the connector wrote nothing.
        SourceRecreatedException ex =
            assertThrows(
                SourceRecreatedException.class, () -> job.runOnce(spark, checkpoint, client));
        assertTrue(ex.getMessage().contains("SOURCE_RECREATED"), ex.getMessage());
        assertEquals(1, node.received.size(), "only the initial build was committed, no stale read");
      }
    } finally {
      server.shutdownNow();
    }
  }

  @Test
  void expectedRowCountGateCountsAppendsAndSeesThroughCompaction() throws Exception {
    spark.sql("DROP TABLE IF EXISTS demo.ns.gate");
    spark.sql("CREATE TABLE demo.ns.gate (id STRING, body STRING) USING iceberg");
    spark.sql("INSERT INTO demo.ns.gate VALUES ('a','1'), ('b','1')");
    spark.sql("INSERT INTO demo.ns.gate VALUES ('c','1'), ('d','1')");

    ConnectorJob job =
        new ConnectorJob(
            "demo",
            "ns.gate",
            new IndexMapping(List.of(), List.of("id"), List.of("id", "body")),
            List.of("id"));

    // Expected = Σ added-records over the window's append snapshots = 4; and the head is resolved
    // from the `main` ref, matching the changelog scan's lineage.
    Long head = job.currentSnapshotId(spark);
    assertEquals(4L, job.expectedAppendedRecords(spark, null, head), "counts both appends");

    // Compaction (a `replace` snapshot) is transparent to the changelog scan AND to the gate: the
    // expected count is unchanged, so a compaction mid-window can't trip a false under-read.
    spark.sql("CALL demo.system.rewrite_data_files(table => 'ns.gate')");
    spark.sql("INSERT INTO demo.ns.gate VALUES ('e','1'), ('f','1')");
    Long head2 = job.currentSnapshotId(spark);
    assertEquals(
        6L,
        job.expectedAppendedRecords(spark, null, head2),
        "replace/compaction contributes 0; only the 6 appended records count");

    // The happy path commits without tripping the gate (changelog rows == expected).
    RecordingWrite node = new RecordingWrite();
    Server server = ServerBuilder.forPort(0).addService(node).build().start();
    try (WriteClient client = new WriteClient("127.0.0.1", server.getPort())) {
      ConnectorJob.Result r = job.runOnce(spark, null, client);
      assertTrue(r.wrote, "the full append window commits");
      assertEquals(head2, r.checkpointSnapshotId, "advances to the refs head");
    } finally {
      server.shutdownNow();
    }
  }

  @Test
  void underReadGateIsExemptForWindowsWithRowLevelUpdatesOrDeletes() throws Exception {
    spark.sql("DROP TABLE IF EXISTS demo.ns.mixed");
    spark.sql("CREATE TABLE demo.ns.mixed (id STRING, body STRING) USING iceberg");
    spark.sql("INSERT INTO demo.ns.mixed VALUES ('a','1'), ('b','1')");
    // An UPDATE / DELETE produces an overwrite/delete snapshot, whose changelog net diff legitimately
    // diverges from physical `added-records` (a row-level delete adds a DELETE row, an update rewrites
    // a data file). The gate must NOT strict-count such a window (that would false-stall); it returns
    // -1 (exempt) and reconcile is the backstop there.
    spark.sql("UPDATE demo.ns.mixed SET body = '2' WHERE id = 'a'");
    spark.sql("DELETE FROM demo.ns.mixed WHERE id = 'b'");

    ConnectorJob job =
        new ConnectorJob(
            "demo",
            "ns.mixed",
            new IndexMapping(List.of(), List.of("id"), List.of("id", "body")),
            List.of("id"));

    Long head = job.currentSnapshotId(spark);
    assertEquals(-1L, job.expectedAppendedRecords(spark, null, head), "mixed window is exempt");

    // And it commits cleanly (the gate is skipped, not tripped).
    RecordingWrite node = new RecordingWrite();
    Server server = ServerBuilder.forPort(0).addService(node).build().start();
    try (WriteClient client = new WriteClient("127.0.0.1", server.getPort())) {
      assertTrue(job.runOnce(spark, null, client).wrote, "mixed window commits without a false stall");
    } finally {
      server.shutdownNow();
    }
  }

  /** Index the batch's ops by their single identifier value (`id`) for assertions. */
  private static Map<String, DocOp> byIdentifier(DocBatch batch) {
    return batch.getOpsList().stream()
        .collect(
            java.util.stream.Collectors.toMap(
                op ->
                    op.hasUpsert()
                        ? op.getUpsert().getDoc().getKey().getIdentifier(0).getValue().getStr()
                        : op.getDelete().getIdentifier(0).getValue().getStr(),
                op -> op));
  }

  /** A Node stub that records committed batches and hands back ascending snapshots. */
  private static final class RecordingWrite extends WriteGrpc.WriteImplBase {
    final List<DocBatch> received = new CopyOnWriteArrayList<>();
    private final AtomicLong snapshot = new AtomicLong();

    @Override
    public void write(WriteRequest request, StreamObserver<WriteResponse> responseObserver) {
      received.add(request.getBatch());
      responseObserver.onNext(
          WriteResponse.newBuilder().setSnapshot(snapshot.incrementAndGet()).build());
      responseObserver.onCompleted();
    }
  }
}
