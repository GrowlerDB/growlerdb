package io.growlerdb.trino;

import io.growlerdb.proto.v1.Coordinates;
import io.growlerdb.proto.v1.Field;
import io.growlerdb.proto.v1.SearchHit;
import io.growlerdb.proto.v1.Value;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.util.ArrayList;
import java.util.List;

/**
 * The search-then-join projection: maps GrowlerDB hits to the columns a
 * {@code growlerdb_search(...)} table function returns — one column per composite-key field
 * (partition then identifier) plus a {@code growlerdb_score} double. The key-field name/kind/
 * value logic is pure (Trino-type-free) so it's unit-testable; {@link #trinoType} bridges the small
 * fixed kind set to Trino's type singletons for the returned descriptor + page building.
 */
public final class SearchColumns {

  /** The relevance-score column carried alongside the key fields. */
  public static final String SCORE_COLUMN = "growlerdb_score";

  /** The fixed set of column types a GrowlerDB key field maps to (serializable in the handle). */
  public enum Kind {
    VARCHAR,
    BIGINT,
    DOUBLE,
    BOOLEAN,
    /** A temporal key (wire {@code ts_micros}) — canonical epoch micros UTC. */
    TIMESTAMP
  }

  private SearchColumns() {}

  /** Column names: each key field (partition ++ identifier) from the first hit, then the score. */
  public static List<String> columnNames(List<SearchHit> hits) {
    List<String> names = new ArrayList<>();
    if (!hits.isEmpty()) {
      for (Field f : keyFields(hits.get(0).getCoordinates())) {
        names.add(f.getName());
      }
    }
    names.add(SCORE_COLUMN);
    return names;
  }

  /** Column kinds aligned with {@link #columnNames}: the key fields' kinds, then {@code DOUBLE}. */
  public static List<Kind> columnKinds(List<SearchHit> hits) {
    List<Kind> kinds = new ArrayList<>();
    if (!hits.isEmpty()) {
      for (Field f : keyFields(hits.get(0).getCoordinates())) {
        kinds.add(kindOf(f.getValue()));
      }
    }
    kinds.add(Kind.DOUBLE); // score
    return kinds;
  }

  /** One hit's cell values aligned with the columns: key field values, then the score double. */
  public static Object[] rowValues(SearchHit hit, int columnCount) {
    Object[] cells = new Object[columnCount];
    List<Field> kf = keyFields(hit.getCoordinates());
    for (int i = 0; i < kf.size() && i < columnCount - 1; i++) {
      cells[i] = javaValue(kf.get(i).getValue());
    }
    cells[columnCount - 1] = hit.getScore();
    return cells;
  }

  /** The kind a key field's value maps to (string keys → VARCHAR, etc.). */
  public static Kind kindOf(Value v) {
    switch (v.getKindCase()) {
      case INT:
        return Kind.BIGINT;
      case FLOAT:
        return Kind.DOUBLE;
      case BOOL:
        return Kind.BOOLEAN;
      case TS_MICROS:
        return Kind.TIMESTAMP;
      case STR:
      default:
        return Kind.VARCHAR;
    }
  }

  /** The Java value behind a key field (matches the {@link Kind}: String/Long/Double/Boolean). */
  public static Object javaValue(Value v) {
    switch (v.getKindCase()) {
      case INT:
        return v.getInt();
      case FLOAT:
        return v.getFloat();
      case BOOL:
        return v.getBool();
      case TS_MICROS:
        // Epoch micros — also TIMESTAMP_MICROS's native long representation (a "short" timestamp).
        return v.getTsMicros();
      case STR:
        return v.getStr();
      default:
        return null;
    }
  }

  /** The Trino {@link Type} for a column kind. */
  public static Type trinoType(Kind kind) {
    switch (kind) {
      case BIGINT:
        return BigintType.BIGINT;
      case DOUBLE:
        return DoubleType.DOUBLE;
      case BOOLEAN:
        return BooleanType.BOOLEAN;
      case TIMESTAMP:
        return TimestampType.TIMESTAMP_MICROS;
      case VARCHAR:
      default:
        return VarcharType.VARCHAR;
    }
  }

  private static List<Field> keyFields(Coordinates c) {
    List<Field> fields = new ArrayList<>(c.getPartitionList());
    fields.addAll(c.getIdentifierList());
    return fields;
  }
}
