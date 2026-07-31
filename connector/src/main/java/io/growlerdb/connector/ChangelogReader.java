package io.growlerdb.connector;

import io.growlerdb.proto.v1.Value;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

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
    return changelog(spark, catalog, table, startSnapshotId, identifierColumns, null);
  }

  /**
   * As {@link #changelog(SparkSession, String, String, Long, List)}, but when {@code variantColumn}
   * is non-null that column is projected as {@code to_json(col)} so it reaches the driver as a JSON
   * <b>string</b> — {@link VariantExtractor} parses it (avoiding the {@code VariantVal} external
   * type) into flatten leaves + shaped values (D47/D48). {@code create_changelog_view} handles
   * variant columns (verified), so this reuses the normal changelog path.
   */
  public static Dataset<Row> changelog(
      SparkSession spark,
      String catalog,
      String table,
      Long startSnapshotId,
      List<String> identifierColumns,
      String variantColumn) {
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
    Dataset<Row> df = spark.table(view);
    if (variantColumn != null) {
      // Render the variant column as JSON text so it arrives as a String (VariantExtractor parses
      // it); to_json handles the shredded/unshredded VARIANT uniformly.
      df = df.withColumn(variantColumn, functions.to_json(functions.col(variantColumn)));
    }
    return df.orderBy("_change_ordinal");
  }

  /**
   * Stream the changelog DataFrame as {@link ChangelogRow}s, pulling <b>one partition at a time</b>
   * to the driver ({@link Dataset#toLocalIterator}) so driver memory stays O(chunk) — a large
   * post-outage backlog does not OOM the driver. Ordered by {@code _change_ordinal}, so iterating
   * partitions in index order preserves changelog order. Locator is a placeholder (see class docs).
   */
  public static Iterator<ChangelogRow> rowIterator(Dataset<Row> changelog, List<String> columns) {
    return rowIterator(changelog, columns, null);
  }

  /**
   * As {@link #rowIterator(Dataset, List)}, but with a {@code variant} extractor: each row's variant
   * column (a JSON string, see the {@code variantColumn} overload of {@link #changelog}) is walked
   * into flatten leaves + discriminator-selected shape values (D47/D48). {@code null} = no variant.
   */
  public static Iterator<ChangelogRow> rowIterator(
      Dataset<Row> changelog, List<String> columns, VariantExtractor variant) {
    Iterator<Row> rows = changelog.toLocalIterator();
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return rows.hasNext();
      }

      @Override
      public ChangelogRow next() {
        return toRow(rows.next(), columns, variant);
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
    return toRow(row, columns, null);
  }

  /** JSON parser for the variant column (rendered as a JSON string by the {@code changelog}
   *  overload). Thread-safe once configured; the driver reads rows single-threaded per partition. */
  private static final com.fasterxml.jackson.databind.ObjectMapper VARIANT_JSON =
      new com.fasterxml.jackson.databind.ObjectMapper();

  /** As {@link #toRow(Row, List)}, but with a {@code variant} extractor (D47/D48): the variant column
   *  is walked into flatten leaves (carried on the {@link ChangelogRow}) plus discriminator-selected
   *  shape values (merged into {@code columns}, riding the typed fields). {@code null} = no variant. */
  static ChangelogRow toRow(Row row, List<String> columns, VariantExtractor variant) {
    ChangeType type = ChangeType.fromIceberg(row.getAs("_change_type"));
    long ordinal = ((Number) row.getAs("_change_ordinal")).longValue();
    long snapshot = ((Number) row.getAs("_commit_snapshot_id")).longValue();
    String variantColumn = variant == null ? null : variant.spec().column;
    var cols = new java.util.HashMap<String, Value>();
    for (String column : columns) {
      // The variant column and its shaped sub-paths aren't real changelog columns — they live inside
      // the variant and are produced by the extractor below (row.getAs would fail). Skip them here.
      if (variantColumn != null
          && (column.equals(variantColumn) || column.startsWith(variantColumn + "."))) {
        continue;
      }
      Object value = row.getAs(column);
      if (value != null) {
        cols.put(column, toValue(value));
      }
    }
    java.util.List<io.growlerdb.proto.v1.VariantColumn> variants = java.util.List.of();
    if (variant != null) {
      Object raw = row.getAs(variantColumn);
      String jsonText = raw == null ? null : raw.toString();
      com.fasterxml.jackson.databind.JsonNode json = null;
      if (jsonText != null && !jsonText.isEmpty()) {
        try {
          json = VARIANT_JSON.readTree(jsonText);
        } catch (Exception e) {
          json = null; // an unparseable variant value → no leaves for this row (still ingested)
        }
      }
      String discriminatorValue = resolveDiscriminator(row, json, variant.spec());
      VariantExtractor.Extracted extracted = variant.extract(json, discriminatorValue);
      cols.putAll(extracted.shapedFields);
      variants = java.util.List.of(extracted.variantColumn);
    }
    return new ChangelogRow(type, ordinal, snapshot, cols, "", 0, variants);
  }

  /** Resolve a row's discriminator value: a path inside the variant (qualified with the column)
   *  reads from the parsed variant JSON; anything else is a sibling column read off the row. */
  private static String resolveDiscriminator(
      Row row, com.fasterxml.jackson.databind.JsonNode json, VariantSpec spec) {
    if (spec.discriminator == null) {
      return null;
    }
    String prefix = spec.column + ".";
    if (spec.discriminator.startsWith(prefix)) {
      com.fasterxml.jackson.databind.JsonNode n = json;
      for (String seg : spec.discriminator.substring(prefix.length()).split("\\.")) {
        n = n == null ? null : n.get(seg);
      }
      return n == null || n.isNull() ? null : n.asText();
    }
    Object v;
    try {
      v = row.getAs(spec.discriminator);
    } catch (IllegalArgumentException missing) {
      return null; // discriminator column not in the changelog projection
    }
    return v == null ? null : v.toString();
  }

  /** Microseconds per UTC day — Spark DATE (days since epoch) → canonical micros. */
  private static final long MICROS_PER_DAY = 86_400_000_000L;

  /**
   * Map a Spark scalar to a wire {@link Value}. Temporal scalars map to {@code ts_micros} — canonical
   * <b>epoch microseconds UTC</b>, matching what the Rust source extracts ({@code Value::Ts}) — so a
   * temporal key encodes/routes identically on both sides instead of stringifying. Spark's external
   * types ({@code java.sql.Date}/{@code Timestamp}, or the {@code java.time} ones under the java8 API)
   * all normalize to the same micros.
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
