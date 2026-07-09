package io.growlerdb.connector;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Self-contained Spark demo proving the changelog read (task-63), run via
 * {@code spark-submit} in Compose (the {@code spark} profile). Against a local
 * Hadoop catalog it creates a table, applies INSERT / UPDATE / DELETE, reads the
 * changelog through {@link ChangelogReader}, and asserts the emitted change types
 * — exiting non-zero on mismatch (so {@code --exit-code-from} gives pass/fail).
 *
 * <p>Polaris/MinIO interop (AC4) is exercised by the connector E2E (task-11),
 * which reads the seeded table from the real catalog.
 */
public final class ChangelogReadApp {

  public static void main(String[] args) {
    String warehouse = args.length > 0 ? args[0] : "/tmp/growlerdb-spark-warehouse";
    SparkSession spark =
        SparkSession.builder()
            .appName("growlerdb-changelog-demo")
            .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.demo.type", "hadoop")
            .config("spark.sql.catalog.demo.warehouse", warehouse)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .getOrCreate();

    int exit = 1;
    try {
      spark.sql("DROP TABLE IF EXISTS demo.ns.docs");
      spark.sql("CREATE TABLE demo.ns.docs (id STRING, body STRING) USING iceberg");
      // Content changes the changelog must surface.
      spark.sql("INSERT INTO demo.ns.docs VALUES ('doc-1','hello'), ('doc-2','world')");
      spark.sql("UPDATE demo.ns.docs SET body = 'updated' WHERE id = 'doc-1'");
      spark.sql("DELETE FROM demo.ns.docs WHERE id = 'doc-2'");

      Dataset<Row> changelog =
          ChangelogReader.changelog(spark, "demo", "ns.docs", null, List.of("id"));
      List<ChangelogRow> rows = ChangelogReader.toRows(changelog, List.of("id", "body"));

      Map<ChangeType, Integer> counts = new TreeMap<>();
      for (ChangelogRow r : rows) {
        counts.merge(r.changeType, 1, Integer::sum);
        System.out.printf(
            "  %-14s ordinal=%d snapshot=%d id=%s body=%s%n",
            r.changeType,
            r.changeOrdinal,
            r.commitSnapshotId,
            r.columns.get("id").getStr(),
            r.columns.containsKey("body") ? r.columns.get("body").getStr() : "∅");
      }
      System.out.println("changelog change-type counts: " + counts);

      boolean hasInsert = counts.containsKey(ChangeType.INSERT);
      boolean hasDelete = counts.containsKey(ChangeType.DELETE);
      boolean hasUpdate =
          counts.containsKey(ChangeType.UPDATE_AFTER) || counts.containsKey(ChangeType.UPDATE_BEFORE);
      if (hasInsert && hasDelete && hasUpdate) {
        System.out.println("OK: changelog surfaced INSERT + UPDATE + DELETE");
        exit = 0;
      } else {
        System.err.printf(
            "FAIL: missing change types (insert=%b update=%b delete=%b)%n",
            hasInsert, hasUpdate, hasDelete);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      spark.stop();
    }
    System.exit(exit);
  }
}
