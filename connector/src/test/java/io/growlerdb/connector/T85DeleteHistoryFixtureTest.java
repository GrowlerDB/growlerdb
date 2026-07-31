package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Fixture generator (task T85): produce a table that genuinely carries an Iceberg merge-on-read
 * positional delete file in its history — the shape needed to settle whether iceberg-rust 0.9.1
 * mis-reads a delete-in-history source. pyiceberg can't make one (copy-on-write rewrites data files).
 *
 * <p>Findings: a no-op {@code DELETE} (zero rows) writes no delete file even under merge-on-read
 * (Spark skips it); a row-matching {@code DELETE} between two appends writes a real positional delete
 * file, and iceberg-rust 0.9.1 reads the correct 9 live rows — the delete-scoping bug does not reproduce.
 *
 * <p>Writes {@code demo.ns.t85} (merge-on-read): append(r0..r4) → DELETE id='r2' → append(r5..r9),
 * leaving 9 live rows + 1 delete file. Set {@code T85_WAREHOUSE} to emit to a stable dir a Rust reader
 * can open. {@code @Tag("fixturegen")} — run with {@code mvn test -Dgroups=fixturegen -Dtest.excludedGroups=}.
 */
@Tag("integration") // heavy (Spark/Iceberg runtime); excluded from the default `mvn verify`
@Tag("fixturegen")
class T85DeleteHistoryFixtureTest {

  @Test
  void sparkWritesMergeOnReadDeleteFileInHistory() throws IOException {
    String fixed = System.getenv("T85_WAREHOUSE");
    Path warehouse =
        fixed != null ? Files.createDirectories(Path.of(fixed)) : Files.createTempDirectory("growlerdb-t85");
    SparkSession spark =
        SparkSession.builder()
            .appName("growlerdb-t85-fixture")
            .master("local[2]")
            .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.demo.type", "hadoop")
            .config("spark.sql.catalog.demo.warehouse", warehouse.toString())
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.ui.enabled", "false")
            .getOrCreate();
    try {
      spark.sql("DROP TABLE IF EXISTS demo.ns.t85");
      // format-version 2 + merge-on-read so DELETE writes a delete *file*, not a CoW rewrite.
      spark.sql(
          "CREATE TABLE demo.ns.t85 (id STRING, val BIGINT) USING iceberg "
              + "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')");
      spark.sql("INSERT INTO demo.ns.t85 VALUES ('r0',0),('r1',1),('r2',2),('r3',3),('r4',4)");
      // A row-matching MoR delete mid-history writes a real positional delete file (a no-op delete
      // matching zero rows writes nothing — Spark skips it). Leaves 9 live rows before the append below.
      spark.sql("DELETE FROM demo.ns.t85 WHERE id = 'r2'");
      spark.sql("INSERT INTO demo.ns.t85 VALUES ('r5',5),('r6',6),('r7',7),('r8',8),('r9',9)");
      long expectedLive = 9L; // r0,r1,r3,r4 + r5..r9

      System.out.println("=== t85 warehouse: " + warehouse);
      System.out.println("=== snapshot history:");
      spark.sql("SELECT operation, snapshot_id, parent_id FROM demo.ns.t85.snapshots").show(false);
      System.out.println("=== data + delete files (.files):");
      spark.sql("SELECT content, file_path, record_count FROM demo.ns.t85.files").show(false);

      long deleteFiles =
          spark.sql("SELECT * FROM demo.ns.t85.files WHERE content != 0").count();
      System.out.println("=== delete-file count: " + deleteFiles);

      long rows = spark.sql("SELECT count(*) FROM demo.ns.t85").collectAsList().get(0).getLong(0);
      assertEquals(expectedLive, rows, "Spark itself must read the 9 live rows back");

      // A real MoR writer leaves a delete file in history (pyiceberg does not); > 0 means the
      // iceberg-rust 0.9.1 delete-scoping path is reachable.
      assertTrue(deleteFiles >= 1, "Spark MoR DELETE must write a positional delete file");

      // Locate the metadata.json so a Rust reader can open it via StaticTable.
      Path tableDir = warehouse.resolve("ns").resolve("t85").resolve("metadata");
      try (var stream = Files.list(tableDir)) {
        stream
            .filter(p -> p.getFileName().toString().endsWith(".metadata.json"))
            .sorted()
            .forEach(p -> System.out.println("=== metadata.json: " + p));
      }
    } finally {
      spark.stop();
    }
  }
}
