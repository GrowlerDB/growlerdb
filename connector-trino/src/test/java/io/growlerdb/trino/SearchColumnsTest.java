package io.growlerdb.trino;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.growlerdb.proto.v1.Coordinates;
import io.growlerdb.proto.v1.Field;
import io.growlerdb.proto.v1.SearchHit;
import io.growlerdb.proto.v1.Value;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.VarcharType;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The search-then-join column projection behind {@code growlerdb_search} (task-51). */
class SearchColumnsTest {

  private static SearchHit hit(String id, long day, double score) {
    return SearchHit.newBuilder()
        .setCoordinates(
            Coordinates.newBuilder()
                .addPartition(
                    Field.newBuilder().setName("day").setValue(Value.newBuilder().setInt(day)))
                .addIdentifier(
                    Field.newBuilder().setName("id").setValue(Value.newBuilder().setStr(id))))
        .setScore(score)
        .build();
  }

  @Test
  void namesKindsAndValuesProjectKeyFieldsThenScore() {
    List<SearchHit> hits = List.of(hit("d1", 20, 2.0), hit("d2", 21, 1.0));

    assertEquals(List.of("day", "id", SearchColumns.SCORE_COLUMN), SearchColumns.columnNames(hits));
    assertEquals(
        List.of(SearchColumns.Kind.BIGINT, SearchColumns.Kind.VARCHAR, SearchColumns.Kind.DOUBLE),
        SearchColumns.columnKinds(hits));

    Object[] row = SearchColumns.rowValues(hits.get(0), 3);
    assertEquals(20L, row[0]);
    assertEquals("d1", row[1]);
    assertEquals(2.0, row[2]);
  }

  @Test
  void kindsMapToTrinoTypes() {
    assertEquals(BigintType.BIGINT, SearchColumns.trinoType(SearchColumns.Kind.BIGINT));
    assertEquals(VarcharType.VARCHAR, SearchColumns.trinoType(SearchColumns.Kind.VARCHAR));
    assertEquals(DoubleType.DOUBLE, SearchColumns.trinoType(SearchColumns.Kind.DOUBLE));
    assertEquals(
        io.trino.spi.type.TimestampType.TIMESTAMP_MICROS,
        SearchColumns.trinoType(SearchColumns.Kind.TIMESTAMP));
  }

  @Test
  void temporalKeyFieldsProjectAsTimestampMicros() {
    // A ts_micros key field (task-184) surfaces as a TIMESTAMP(6) column whose native value is
    // the canonical epoch-micros long itself.
    long micros = 1_782_000_123_456_789L;
    SearchHit h =
        SearchHit.newBuilder()
            .setCoordinates(
                Coordinates.newBuilder()
                    .addIdentifier(
                        Field.newBuilder()
                            .setName("ts")
                            .setValue(Value.newBuilder().setTsMicros(micros))))
            .setScore(1.0)
            .build();
    assertEquals(
        List.of(SearchColumns.Kind.TIMESTAMP, SearchColumns.Kind.DOUBLE),
        SearchColumns.columnKinds(List.of(h)));
    assertEquals(micros, SearchColumns.rowValues(h, 2)[0]);
  }

  @Test
  void emptyResultIsAScoreOnlyRelation() {
    assertEquals(List.of(SearchColumns.SCORE_COLUMN), SearchColumns.columnNames(List.of()));
    assertEquals(List.of(SearchColumns.Kind.DOUBLE), SearchColumns.columnKinds(List.of()));
  }
}
