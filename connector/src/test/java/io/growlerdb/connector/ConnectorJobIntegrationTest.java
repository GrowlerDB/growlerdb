package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.growlerdb.proto.v1.DocBatch;
import io.growlerdb.proto.v1.DocOp;
import io.growlerdb.proto.v1.LocatedDoc;
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
  void backfillEmitsRealLocatorsThenIncrementalUsesPlaceholder() throws Exception {
    spark.sql("DROP TABLE IF EXISTS demo.ns.docs");
    spark.sql("CREATE TABLE demo.ns.docs (id STRING, body STRING) USING iceberg");
    spark.sql("INSERT INTO demo.ns.docs VALUES ('doc-1','hello'), ('doc-2','world')");

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
        // From-empty (null checkpoint) → BACKFILL: a plain scan of the current snapshot, every row an
        // upsert carrying its REAL (file, position) so hydration can pass-1 point-read.
        ConnectorJob.Result backfill = job.runOnce(spark, null, client);
        assertTrue(backfill.wrote, "a backfill batch should be committed");
        assertEquals(backfill.checkpointSnapshotId, job.currentSnapshotId(spark));

        assertEquals(1, node.received.size(), "backfill sent one bounded sub-batch");
        DocBatch backfillBatch = node.received.get(0);
        // A bootstrap sub-batch: no `from` (covers from empty), checkpoint = current.
        assertFalse(backfillBatch.hasFromCheckpoint(), "backfill sub-batch is a bootstrap (no `from`)");
        Map<String, DocOp> backfilled = byIdentifier(backfillBatch);
        for (String id : List.of("doc-1", "doc-2")) {
          DocOp op = backfilled.get(id);
          assertTrue(op.hasUpsert(), id + " → upsert");
          LocatedDoc located = op.getUpsert();
          assertFalse(
              located.getIcebergFile().isEmpty(), id + " carries a REAL data-file path, not \"\"");
          assertTrue(located.getRowPosition() >= 0, id + " carries a real row position");
        }

        // Now an UPDATE + DELETE, then resume INCREMENTALLY (a non-null checkpoint) → the changelog
        // path, which emits the placeholder locator (hydration heals it lazily).
        spark.sql("UPDATE demo.ns.docs SET body = 'updated' WHERE id = 'doc-1'");
        spark.sql("DELETE FROM demo.ns.docs WHERE id = 'doc-2'");
        ConnectorJob.Result incr = job.runOnce(spark, backfill.checkpointSnapshotId, client);
        assertTrue(incr.wrote, "the update/delete window commits");

        assertEquals(2, node.received.size(), "incremental sent a second batch");
        Map<String, DocOp> changed = byIdentifier(node.received.get(1));

        DocOp doc1 = changed.get("doc-1");
        assertTrue(doc1.hasUpsert(), "doc-1 → upsert (UPDATE_AFTER)");
        assertEquals("updated", doc1.getUpsert().getDoc().getFieldsMap().get("body").getStr());
        assertTrue(
            doc1.getUpsert().getIcebergFile().isEmpty(),
            "incremental changelog upsert keeps the placeholder locator");

        DocOp doc2 = changed.get("doc-2");
        assertTrue(doc2.hasDelete(), "doc-2 → delete");

        // Idempotent resume: re-running from the same checkpoint is a no-op.
        assertFalse(
            job.runOnce(spark, incr.checkpointSnapshotId, client).wrote,
            "already caught up → no RPC");
      }
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
    // UPDATE/DELETE snapshots have a changelog net diff that diverges from physical added-records, so
    // the gate must not strict-count them (would false-stall): it returns -1 (exempt), reconcile backstops.
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
