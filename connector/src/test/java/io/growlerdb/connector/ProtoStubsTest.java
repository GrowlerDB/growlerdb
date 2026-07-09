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
import org.junit.jupiter.api.Test;

/**
 * Confirms the {@code growlerdb.v1} Java stubs generate and build a {@link DocBatch}
 * — i.e. the scaffold + gRPC codegen from the shared protos works end to end.
 */
class ProtoStubsTest {

  @Test
  void buildsDocBatchFromGeneratedStubs() {
    Value id = Value.newBuilder().setStr("doc-1").build();
    Coordinates key =
        Coordinates.newBuilder()
            .addIdentifier(Field.newBuilder().setName("id").setValue(id))
            .build();
    Document doc =
        Document.newBuilder().setKey(key).putFields("body", id).build();
    DocOp upsert =
        DocOp.newBuilder()
            .setUpsert(
                LocatedDoc.newBuilder()
                    .setDoc(doc)
                    .setIcebergFile("data/f0.parquet")
                    .setRowPosition(0))
            .build();
    DocBatch batch =
        DocBatch.newBuilder()
            .addOps(upsert)
            .setCheckpoint(SourceCheckpoint.newBuilder().setIcebergSnapshot(7))
            .setBatchId("b1")
            .build();

    assertEquals(1, batch.getOpsCount());
    assertEquals("b1", batch.getBatchId());
    assertEquals(7, batch.getCheckpoint().getIcebergSnapshot());
    assertTrue(batch.getOps(0).hasUpsert());
    assertEquals("doc-1", batch.getOps(0).getUpsert().getDoc().getKey().getIdentifier(0).getValue().getStr());
  }
}
