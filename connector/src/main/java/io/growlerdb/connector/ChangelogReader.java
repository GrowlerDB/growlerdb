package io.growlerdb.connector;

import io.growlerdb.proto.v1.Value;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Reads the Iceberg **changelog** for a table via Iceberg's
 * {@code create_changelog_view} procedure (the {@code IncrementalChangelogScan}),
 * yielding rows tagged {@code _change_type} / {@code _change_ordinal} /
 * {@code _commit_snapshot_id} plus the table columns. The row→{@code DocOp}
 * mapping is {@link ChangelogMapper}.
 *
 * <p>The changelog scan abstracts away data-file layout, so it does not carry the
 * source {@code (file, position)}; {@link #toRows} emits a <b>placeholder
 * locator</b> and hydration fills it lazily via verify-and-fall-back.
 */
public final class ChangelogReader {

  private ChangelogReader() {}

  /**
   * Build the changelog DataFrame for {@code table} in {@code catalog} between
   * {@code startSnapshotId} (exclusive; {@code null} = from the start) and the
   * current snapshot. {@code identifierColumns} pair UPDATE_BEFORE/UPDATE_AFTER.
   */
  public static Dataset<Row> changelog(
      SparkSession spark,
      String catalog,
      String table,
      Long startSnapshotId,
      List<String> identifierColumns) {
    String view = "growlerdb_changelog_view";
    String ids =
        identifierColumns.stream()
            .map(c -> "'" + c + "'")
            .collect(Collectors.joining(", ", "array(", ")"));
    StringBuilder options = new StringBuilder();
    if (startSnapshotId != null) {
      options.append(", options => map('start-snapshot-id', '").append(startSnapshotId).append("')");
    }
    spark.sql(
        "CALL "
            + catalog
            + ".system.create_changelog_view("
            + "table => '" + table + "'"
            + ", changelog_view => '" + view + "'"
            + ", compute_updates => true"
            + ", identifier_columns => " + ids
            + options
            + ")");
    return spark.table(view).orderBy("_change_ordinal");
  }

  /**
   * Stream the changelog DataFrame as {@link ChangelogRow}s, pulling <b>one partition at a time</b>
   * to the driver ({@link Dataset#toLocalIterator}) rather than the whole window at once. Combined
   * with the bounded read→map→commit loop, this keeps driver memory O(chunk) — a large post-outage
   * backlog does not OOM the driver. The DataFrame is ordered by {@code _change_ordinal} (a global
   * sort's range partitioning), so iterating partitions in index order preserves changelog order.
   * Locator is a placeholder (see class docs).
   */
  public static Iterator<ChangelogRow> rowIterator(Dataset<Row> changelog, List<String> columns) {
    Iterator<Row> rows = changelog.toLocalIterator();
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return rows.hasNext();
      }

      @Override
      public ChangelogRow next() {
        return toRow(rows.next(), columns);
      }
    };
  }

  /**
   * Materialize the whole changelog DataFrame as {@link ChangelogRow}s — drains
   * {@link #rowIterator}. Used by the read-only inspection app and tests; the ingestion path
   * streams via {@link #rowIterator} instead.
   */
  public static List<ChangelogRow> toRows(Dataset<Row> changelog, List<String> columns) {
    List<ChangelogRow> rows = new ArrayList<>();
    rowIterator(changelog, columns).forEachRemaining(rows::add);
    return rows;
  }

  /** Map one changelog {@link Row} to a {@link ChangelogRow}, projecting {@code columns} as wire
   *  {@link Value}s. Placeholder locator (hydration fills it lazily). */
  static ChangelogRow toRow(Row row, List<String> columns) {
    ChangeType type = ChangeType.fromIceberg(row.getAs("_change_type"));
    long ordinal = ((Number) row.getAs("_change_ordinal")).longValue();
    long snapshot = ((Number) row.getAs("_commit_snapshot_id")).longValue();
    var cols = new java.util.HashMap<String, Value>();
    for (String column : columns) {
      Object value = row.getAs(column);
      if (value != null) {
        cols.put(column, toValue(value));
      }
    }
    return new ChangelogRow(type, ordinal, snapshot, cols, "", 0);
  }

  /** Microseconds per UTC day — Spark DATE (days since epoch) → canonical micros. */
  private static final long MICROS_PER_DAY = 86_400_000_000L;

  /**
   * Map a Spark scalar to a wire {@link Value}. Temporal scalars map to
   * {@code ts_micros} — canonical <b>epoch microseconds UTC</b>, matching what the
   * Rust source extracts ({@code Value::Ts}) — so a temporal key encodes/routes identically on
   * both sides instead of stringifying via {@code toString()}. Spark's external row types are
   * {@code java.sql.Date}/{@code Timestamp} (or {@code java.time.LocalDate}/{@code Instant}
   * with {@code spark.sql.datetime.java8API.enabled}); all normalize to the same micros.
   */
  static Value toValue(Object value) {
    if (value instanceof String s) {
      return Value.newBuilder().setStr(s).build();
    }
    if (value instanceof Long l) {
      return Value.newBuilder().setInt(l).build();
    }
    if (value instanceof Integer i) {
      return Value.newBuilder().setInt(i.longValue()).build();
    }
    if (value instanceof Double d) {
      return Value.newBuilder().setFloat(d).build();
    }
    if (value instanceof Float f) {
      return Value.newBuilder().setFloat(f.doubleValue()).build();
    }
    if (value instanceof Boolean b) {
      return Value.newBuilder().setBool(b).build();
    }
    if (value instanceof java.sql.Date d) {
      // java.sql.Date carries a local-calendar day; toLocalDate() recovers it, epochDay × µs/day
      // is the same UTC-midnight instant Rust derives from Iceberg's days-since-epoch DATE.
      return Value.newBuilder().setTsMicros(d.toLocalDate().toEpochDay() * MICROS_PER_DAY).build();
    }
    if (value instanceof java.time.LocalDate d) {
      return Value.newBuilder().setTsMicros(d.toEpochDay() * MICROS_PER_DAY).build();
    }
    if (value instanceof java.sql.Timestamp t) {
      // getTime() is epoch millis (incl. the ms part of the fraction); add the sub-ms micros.
      long micros = t.getTime() * 1_000L + (t.getNanos() / 1_000L) % 1_000L;
      return Value.newBuilder().setTsMicros(micros).build();
    }
    if (value instanceof java.time.Instant t) {
      long micros = Math.addExact(Math.multiplyExact(t.getEpochSecond(), 1_000_000L), t.getNano() / 1_000L);
      return Value.newBuilder().setTsMicros(micros).build();
    }
    if (value instanceof java.time.LocalDateTime t) {
      // TIMESTAMP_NTZ external type: no zone by definition — taken at UTC, matching the engine's
      // canonical-micros convention for zoneless timestamps.
      return toValue(t.toInstant(java.time.ZoneOffset.UTC));
    }
    return Value.newBuilder().setStr(value.toString()).build();
  }
}
