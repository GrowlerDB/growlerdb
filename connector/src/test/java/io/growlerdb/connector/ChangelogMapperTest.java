package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.growlerdb.proto.v1.DocBatch;
import io.growlerdb.proto.v1.DocOp;
import io.growlerdb.proto.v1.SourceCheckpoint;
import io.growlerdb.proto.v1.Value;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ChangelogMapperTest {

  private static final IndexMapping DOCS =
      new IndexMapping(List.of(), List.of("id"), List.of("id", "body"));
  private static final SourceCheckpoint CP =
      SourceCheckpoint.newBuilder().setIcebergSnapshot(1).build();

  private static Value str(String s) {
    return Value.newBuilder().setStr(s).build();
  }

  private static ChangelogRow row(
      ChangeType type, long ordinal, long snapshot, String id, String body) {
    Map<String, Value> cols = new HashMap<>();
    cols.put("id", str(id));
    cols.put("body", str(body));
    return new ChangelogRow(type, ordinal, snapshot, cols, "data/f0.parquet", ordinal);
  }

  private static ChangelogMapper mapper() {
    return new ChangelogMapper(DOCS, Set.of());
  }

  private static String upsertId(DocOp op) {
    return op.getUpsert().getDoc().getKey().getIdentifier(0).getValue().getStr();
  }

  @Test
  void insertMapsToUpsert() {
    DocBatch batch =
        mapper().toBatch(List.of(row(ChangeType.INSERT, 0, 1, "doc-1", "hello")), CP, "b1");
    assertEquals(1, batch.getOpsCount());
    DocOp op = batch.getOps(0);
    assertTrue(op.hasUpsert());
    assertEquals("doc-1", upsertId(op));
    assertEquals("hello", op.getUpsert().getDoc().getFieldsMap().get("body").getStr());
    assertEquals("data/f0.parquet", op.getUpsert().getIcebergFile());
  }

  @Test
  void equalityDeleteOnKeyMapsToDeleteByKey() {
    // An Iceberg equality delete keyed on `id` surfaces as a DELETE change carrying
    // only the equality (key) column — no full pre-image. GrowlerDB is keyed by the
    // composite key, so it still maps cleanly to delete-by-key.
    Map<String, Value> keyOnly = new HashMap<>();
    keyOnly.put("id", str("doc-1")); // body (a non-key column) is absent
    ChangelogRow eqDelete =
        new ChangelogRow(ChangeType.DELETE, 0, 1, keyOnly, "data/f0.parquet", 0);

    DocBatch batch = mapper().toBatch(List.of(eqDelete), CP, "b1");
    assertEquals(1, batch.getOpsCount());
    DocOp op = batch.getOps(0);
    assertTrue(op.hasDelete(), "equality delete on key → delete-by-key");
    assertEquals("doc-1", op.getDelete().getIdentifier(0).getValue().getStr());
  }

  @Test
  void deleteMapsToDelete() {
    DocBatch batch =
        mapper().toBatch(List.of(row(ChangeType.DELETE, 0, 1, "doc-1", "hello")), CP, "b1");
    assertEquals(1, batch.getOpsCount());
    DocOp op = batch.getOps(0);
    assertTrue(op.hasDelete());
    assertEquals("doc-1", op.getDelete().getIdentifier(0).getValue().getStr());
  }

  @Test
  void updateCollapsesToSingleUpsert() {
    // UPDATE_BEFORE (delete) then UPDATE_AFTER (upsert) for the same key →
    // last-write-wins → one upsert with the new value.
    DocBatch batch =
        mapper()
            .toBatch(
                List.of(
                    row(ChangeType.UPDATE_BEFORE, 0, 1, "doc-1", "old"),
                    row(ChangeType.UPDATE_AFTER, 1, 1, "doc-1", "new")),
                CP,
                "b1");
    assertEquals(1, batch.getOpsCount());
    DocOp op = batch.getOps(0);
    assertTrue(op.hasUpsert());
    assertEquals("new", op.getUpsert().getDoc().getFieldsMap().get("body").getStr());
  }

  @Test
  void lastWriteWinsRegardlessOfInputOrder() {
    // Out-of-sequence input: the mapper sorts by (snapshot, ordinal).
    DocBatch batch =
        mapper()
            .toBatch(
                List.of(
                    row(ChangeType.INSERT, 1, 1, "doc-1", "second"),
                    row(ChangeType.INSERT, 0, 1, "doc-1", "first")),
                CP,
                "b1");
    assertEquals(1, batch.getOpsCount());
    assertEquals("second", batch.getOps(0).getUpsert().getDoc().getFieldsMap().get("body").getStr());
  }

  @Test
  void orderingFollowsChangeOrdinalNotSnapshotId() {
    // Real Iceberg snapshot ids are random longs, not monotonic: here the earlier
    // INSERT has a *larger* id than the later UPDATE. Ordering must follow
    // _change_ordinal (commit order), so the UPDATE_AFTER wins — not the insert.
    DocBatch batch =
        mapper()
            .toBatch(
                List.of(
                    row(ChangeType.INSERT, 0, 7001819832461178278L, "doc-1", "hello"),
                    row(ChangeType.UPDATE_BEFORE, 1, 3034960034988343108L, "doc-1", "hello"),
                    row(ChangeType.UPDATE_AFTER, 1, 3034960034988343108L, "doc-1", "updated")),
                CP,
                "b1");
    assertEquals(1, batch.getOpsCount());
    DocOp op = batch.getOps(0);
    assertTrue(op.hasUpsert());
    assertEquals("updated", op.getUpsert().getDoc().getFieldsMap().get("body").getStr());
  }

  @Test
  void replaceSnapshotRowsAreSkipped() {
    // Snapshot 2 is a compaction/replace (layout only) — its rows produce no doc
    // ops; only the genuine content change from snapshot 1 survives.
    ChangelogMapper m = new ChangelogMapper(DOCS, Set.of(2L));
    DocBatch batch =
        m.toBatch(
            List.of(
                row(ChangeType.INSERT, 0, 1, "doc-1", "real change"),
                row(ChangeType.DELETE, 0, 2, "doc-1", "rewritten away"),
                row(ChangeType.INSERT, 1, 2, "doc-1", "rewritten in")),
            CP,
            "b1");
    assertEquals(1, batch.getOpsCount());
    assertTrue(batch.getOps(0).hasUpsert());
    assertEquals("real change", batch.getOps(0).getUpsert().getDoc().getFieldsMap().get("body").getStr());
  }

  @Test
  void stampsResumeFloorWhenSupplied() {
    // The resume floor (`safeCheckpoint`) rides the batch so the Node can prune
    // idempotency records at/below it. A null floor leaves the field unset (prune nothing).
    SourceCheckpoint from = SourceCheckpoint.newBuilder().setIcebergSnapshot(5).build();
    SourceCheckpoint floor = SourceCheckpoint.newBuilder().setIcebergSnapshot(3).build();
    DocBatch batch =
        mapper()
            .toBatch(List.of(row(ChangeType.INSERT, 0, 7, "doc-1", "hi")), from, CP, "b1", floor);
    assertTrue(batch.hasSafeCheckpoint(), "floor stamped");
    assertEquals(3, batch.getSafeCheckpoint().getIcebergSnapshot());

    DocBatch noFloor =
        mapper().toBatch(List.of(row(ChangeType.INSERT, 0, 7, "doc-1", "hi")), from, CP, "b1", null);
    assertTrue(!noFloor.hasSafeCheckpoint(), "no floor supplied → field left unset");
  }
}
