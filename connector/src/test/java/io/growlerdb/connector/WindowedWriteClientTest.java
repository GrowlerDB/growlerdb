package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.growlerdb.proto.v1.Coordinates;
import io.growlerdb.proto.v1.DocBatch;
import io.growlerdb.proto.v1.DocOp;
import io.growlerdb.proto.v1.Document;
import io.growlerdb.proto.v1.Field;
import io.growlerdb.proto.v1.LocatedDoc;
import io.growlerdb.proto.v1.SourceCheckpoint;
import io.growlerdb.proto.v1.Value;
import io.growlerdb.proto.v1.WindowingConfig;
import java.util.Set;
import java.util.SortedMap;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link WindowedWriteClient#partition} — the pure window-routing of a batch. */
class WindowedWriteClientTest {

  private static final long DAY = 86_400_000_000L;

  private static WindowRouter router() {
    return new WindowRouter(
        WindowingConfig.newBuilder()
            .setField("ts")
            .setGranularity("daily")
            .setFieldFormat("epoch_micros")
            .build());
  }

  private static DocOp upsert(String id, long tsMicros) {
    Coordinates key =
        Coordinates.newBuilder()
            .addIdentifier(Field.newBuilder().setName("id").setValue(Value.newBuilder().setStr(id)))
            .build();
    Document doc =
        Document.newBuilder()
            .setKey(key)
            .putFields("id", Value.newBuilder().setStr(id).build())
            .putFields("ts", Value.newBuilder().setInt(tsMicros).build())
            .build();
    return DocOp.newBuilder().setUpsert(LocatedDoc.newBuilder().setDoc(doc)).build();
  }

  private static DocOp delete(String id) {
    return DocOp.newBuilder()
        .setDelete(
            Coordinates.newBuilder()
                .addIdentifier(
                    Field.newBuilder().setName("id").setValue(Value.newBuilder().setStr(id))))
        .build();
  }

  private static DocBatch batch(DocOp... ops) {
    return DocBatch.newBuilder()
        .addAllOps(java.util.Arrays.asList(ops))
        .setCheckpoint(SourceCheckpoint.newBuilder().setIcebergSnapshot(7))
        .setBatchId("b1")
        .build();
  }

  @Test
  void routesUpsertsToTheirWindowWithPerWindowBatchIds() {
    // Two docs on day 10, one on day 11 → two window sub-batches.
    SortedMap<Long, DocBatch> parts =
        WindowedWriteClient.partition(
            batch(upsert("a", 10 * DAY + 5), upsert("b", 11 * DAY + 1), upsert("c", 10 * DAY + 9)),
            router(),
            Set.of());
    assertEquals(2, parts.size());
    assertEquals(2, parts.get(10 * DAY).getOpsCount(), "day-10 window has a + c");
    assertEquals(1, parts.get(11 * DAY).getOpsCount(), "day-11 window has b");
    // Each sub-batch carries the same checkpoint and a per-window batch_id, and NO from checkpoint.
    assertEquals("b1#w" + (10 * DAY), parts.get(10 * DAY).getBatchId());
    assertEquals(7, parts.get(10 * DAY).getCheckpoint().getIcebergSnapshot());
    assertTrue(!parts.get(10 * DAY).hasFromCheckpoint(), "windowed sub-batches use from=None");
  }

  @Test
  void deletesBroadcastToTouchedAndKnownWindows() {
    // A batch with one day-10 upsert + a delete; window 20 is already known (from a prior batch).
    SortedMap<Long, DocBatch> parts =
        WindowedWriteClient.partition(
            batch(upsert("a", 10 * DAY), delete("gone")), router(), Set.of(20 * DAY));
    // The delete reaches the touched window (10) AND the known window (20).
    assertEquals(Set.of(10 * DAY, 20 * DAY), parts.keySet());
    // Window 10: the upsert + the broadcast delete; window 20: just the broadcast delete.
    assertEquals(2, parts.get(10 * DAY).getOpsCount());
    assertEquals(1, parts.get(20 * DAY).getOpsCount());
    assertTrue(parts.get(20 * DAY).getOps(0).hasDelete());
  }

  @Test
  void aDeleteOnlyBatchWithNoKnownWindowsWritesNothing() {
    // Nowhere to route a delete before any window exists → an empty partition (skipped, not an error).
    assertTrue(WindowedWriteClient.partition(batch(delete("x")), router(), Set.of()).isEmpty());
  }

  @Test
  void carriesTheSafeFloorButNeverFrom() {
    // The safe checkpoint (global resume floor) must reach every window sub-batch so each
    // prunes its idempotency records; `from` stays absent (windows advance independently and
    // would false-Gap on it).
    DocBatch withFloors =
        DocBatch.newBuilder()
            .addOps(upsert("a", 10 * DAY))
            .addOps(upsert("b", 11 * DAY))
            .setCheckpoint(SourceCheckpoint.newBuilder().setIcebergSnapshot(9).setIcebergSequenceNumber(9))
            .setFromCheckpoint(
                SourceCheckpoint.newBuilder().setIcebergSnapshot(8).setIcebergSequenceNumber(8))
            .setSafeCheckpoint(
                SourceCheckpoint.newBuilder().setIcebergSnapshot(5).setIcebergSequenceNumber(5))
            .setBatchId("b1")
            .build();
    SortedMap<Long, DocBatch> parts = WindowedWriteClient.partition(withFloors, router(), Set.of());
    assertEquals(2, parts.size());
    for (DocBatch sub : parts.values()) {
      assertTrue(sub.hasSafeCheckpoint(), "resume floor carried per window");
      assertEquals(5, sub.getSafeCheckpoint().getIcebergSnapshot());
      assertTrue(!sub.hasFromCheckpoint(), "windowed sub-batches use from=None");
    }
  }
}
