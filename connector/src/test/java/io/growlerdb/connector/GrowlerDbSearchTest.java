package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.growlerdb.proto.v1.Coordinates;
import io.growlerdb.proto.v1.Field;
import io.growlerdb.proto.v1.SearchHit;
import io.growlerdb.proto.v1.Value;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/**
 * The pure hit→(schema, rows) projection behind {@link GrowlerDbSearch#search} (task-51) — the
 * search-then-join shape, tested without a SparkSession.
 */
class GrowlerDbSearchTest {

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
  void schemaAndRowsProjectKeyFieldsThenScore() {
    List<SearchHit> hits = List.of(hit("d1", 20, 2.0), hit("d2", 21, 1.0));

    // Partition fields first, then identifier fields, each typed from the Value oneof...
    StructType schema = GrowlerDbSearch.schemaFor(hits);
    assertEquals(3, schema.fields().length);
    assertEquals("day", schema.fields()[0].name());
    assertEquals(DataTypes.LongType, schema.fields()[0].dataType());
    assertEquals("id", schema.fields()[1].name());
    assertEquals(DataTypes.StringType, schema.fields()[1].dataType());
    // ...then the score column the join carries for ranking.
    assertEquals(GrowlerDbSearch.SCORE_COLUMN, schema.fields()[2].name());
    assertEquals(DataTypes.DoubleType, schema.fields()[2].dataType());

    List<Row> rows = GrowlerDbSearch.rowsFor(hits, schema);
    assertEquals(2, rows.size());
    assertEquals(20L, rows.get(0).get(0));
    assertEquals("d1", rows.get(0).get(1));
    assertEquals(2.0, rows.get(0).get(2));
    assertEquals("d2", rows.get(1).get(1));
  }

  @Test
  void emptyResultIsAValidScoreOnlyRelation() {
    StructType schema = GrowlerDbSearch.schemaFor(List.of());
    assertEquals(1, schema.fields().length);
    assertEquals(GrowlerDbSearch.SCORE_COLUMN, schema.fields()[0].name());
    assertEquals(0, GrowlerDbSearch.rowsFor(List.of(), schema).size());
  }
}
