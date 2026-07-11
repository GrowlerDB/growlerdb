package io.growlerdb.connector;

import io.growlerdb.proto.v1.Coordinates;
import io.growlerdb.proto.v1.Field;
import io.growlerdb.proto.v1.SearchHit;
import io.growlerdb.proto.v1.Value;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Spark SQL entry point for GrowlerDB retrieval: run a boolean/full-text query against a
 * GrowlerDB index and get the matching document keys + relevance score back as a {@link Dataset} you
 * <b>join against the source Iceberg table</b>. This is the "search-then-join" model:
 * GrowlerDB returns coordinates, the lakehouse engine resolves the authoritative rows.
 *
 * <pre>{@code
 * Dataset<Row> matches = GrowlerDbSearch.search(spark, "gateway-host", 50061,
 *                                               "body:error AND env:prod", 1000);
 * // matches columns: the key fields (e.g. `id`) + a double `growlerdb_score`.
 * Dataset<Row> rows = spark.table("lake.events").join(matches, "id");        // filter to matches
 * Dataset<Row> ranked = rows.orderBy(matches.col(GrowlerDbSearch.SCORE_COLUMN).desc());
 * }</pre>
 *
 * <p>A scalar {@code growlerdb_match} predicate doesn't fit Spark's per-row UDF model (a search runs
 * once, not per row); the join expresses "match" (inner join keeps only hits) and "score" (the
 * carried column) — the two UDFs the task calls for. The hit-to-row mapping is factored into pure,
 * {@code SparkSession}-free helpers so it's unit-testable without a cluster.
 */
public final class GrowlerDbSearch {

  /** The relevance-score column carried alongside the key fields in a result {@link Dataset}. */
  public static final String SCORE_COLUMN = "growlerdb_score";

  private GrowlerDbSearch() {}

  /**
   * Run {@code query} against the GrowlerDB endpoint at {@code host:port} and return the matching
   * keys + scores as a joinable {@link Dataset}. The search executes once on the driver; the results
   * (bounded by {@code limit}) are parallelized for the join.
   */
  public static Dataset<Row> search(
      SparkSession spark, String host, int port, String query, int limit) {
    List<SearchHit> hits;
    try (SearchClient client = new SearchClient(host, port)) {
      hits = client.search(query, limit);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("GrowlerDB search interrupted", e);
    }
    StructType schema = schemaFor(hits);
    return spark.createDataFrame(rowsFor(hits, schema), schema);
  }

  /**
   * The Spark schema for a result set: one column per key field (partition fields then identifier
   * fields), named and typed from the first hit's coordinates, plus a non-null double
   * {@link #SCORE_COLUMN}. Empty hits ⇒ just the score column (a valid empty relation to join).
   */
  public static StructType schemaFor(List<SearchHit> hits) {
    List<StructField> fields = new ArrayList<>();
    if (!hits.isEmpty()) {
      for (Field f : keyFields(hits.get(0).getCoordinates())) {
        fields.add(DataTypes.createStructField(f.getName(), sparkType(f.getValue()), true));
      }
    }
    fields.add(DataTypes.createStructField(SCORE_COLUMN, DataTypes.DoubleType, false));
    return DataTypes.createStructType(fields);
  }

  /** Map each hit to a {@link Row} matching {@code schema} (key field values ++ score). */
  public static List<Row> rowsFor(List<SearchHit> hits, StructType schema) {
    int cols = schema.fields().length;
    List<Row> rows = new ArrayList<>(hits.size());
    for (SearchHit hit : hits) {
      Object[] cells = new Object[cols];
      List<Field> kf = keyFields(hit.getCoordinates());
      for (int i = 0; i < kf.size() && i < cols - 1; i++) {
        cells[i] = javaValue(kf.get(i).getValue());
      }
      cells[cols - 1] = hit.getScore();
      rows.add(RowFactory.create(cells));
    }
    return rows;
  }

  /** A composite key's fields in column order: partition fields, then identifier fields. */
  private static List<Field> keyFields(Coordinates c) {
    List<Field> fields = new ArrayList<>(c.getPartitionList());
    fields.addAll(c.getIdentifierList());
    return fields;
  }

  private static DataType sparkType(Value v) {
    switch (v.getKindCase()) {
      case INT:
        return DataTypes.LongType;
      case FLOAT:
        return DataTypes.DoubleType;
      case BOOL:
        return DataTypes.BooleanType;
      case TS_MICROS:
        return DataTypes.TimestampType;
      case STR:
      default:
        return DataTypes.StringType;
    }
  }

  private static Object javaValue(Value v) {
    switch (v.getKindCase()) {
      case INT:
        return v.getInt();
      case FLOAT:
        return v.getFloat();
      case BOOL:
        return v.getBool();
      case TS_MICROS:
        // Canonical epoch micros → the TimestampType external type.
        long micros = v.getTsMicros();
        return java.sql.Timestamp.from(
            java.time.Instant.ofEpochSecond(
                Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1_000L));
      case STR:
        return v.getStr();
      default:
        return null;
    }
  }
}
