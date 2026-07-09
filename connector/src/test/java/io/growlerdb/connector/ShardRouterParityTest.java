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
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Locks the JVM {@link ShardRouter} to the <b>same golden vectors</b> the Rust router asserts
 * in {@code crates/growlerdb-core/src/routing.rs}. If either side's key encoding, FNV-1a hash,
 * or routing strategy drifts, these numbers diverge and a test fails — rather than the
 * connector silently writing a document to a shard the reader will never query for it.
 */
class ShardRouterParityTest {

  // --- value + key builders ---------------------------------------------------

  private static Value str(String s) {
    return Value.newBuilder().setStr(s).build();
  }

  private static Value i64(long v) {
    return Value.newBuilder().setInt(v).build();
  }

  private static Value f64(double v) {
    return Value.newBuilder().setFloat(v).build();
  }

  private static Value bool(boolean v) {
    return Value.newBuilder().setBool(v).build();
  }

  private static Value ts(long micros) {
    return Value.newBuilder().setTsMicros(micros).build();
  }

  private static Field field(String name, Value v) {
    return Field.newBuilder().setName(name).setValue(v).build();
  }

  private static Coordinates key(List<Field> partition, List<Field> identifier) {
    return Coordinates.newBuilder().addAllPartition(partition).addAllIdentifier(identifier).build();
  }

  /**
   * One golden row: a key + its expected fnv1a(encode), bucket@1024, hashed(8) shard,
   * partitioned(8) shard.
   */
  private record Golden(String name, Coordinates key, long fnv, int bucket, int hash8, int part8) {}

  /**
   * The cross-language contract. These exact numbers are also asserted by the Rust test
   * {@code route_matches_the_cross_language_golden_vectors}; keep both tables in sync.
   */
  private static List<Golden> golden() {
    return List.of(
        new Golden("k1", key(List.of(), List.of(field("id", str("doc-1")))), -2734934794012563331L, 125, 5, 5),
        new Golden("k2", key(List.of(), List.of(field("id", str("doc-2")))), -2734938092547447964L, 868, 4, 4),
        new Golden(
            "k3",
            key(List.of(field("region", str("eu"))), List.of(field("id", str("doc-1")))),
            3928395953618384062L,
            190,
            6,
            0),
        new Golden(
            "k4",
            key(List.of(field("region", str("us"))), List.of(field("id", str("doc-1")))),
            -2015660741178642728L,
            728,
            0,
            2),
        new Golden("k5", key(List.of(), List.of(field("id", i64(42)))), 3679709532207596177L, 657, 1, 1),
        new Golden("k6", key(List.of(), List.of(field("id", f64(3.5)))), -467064767285848684L, 404, 4, 4),
        new Golden("k7", key(List.of(), List.of(field("active", bool(true)))), -8857851673729752600L, 488, 0, 0),
        new Golden(
            "k8",
            key(
                List.of(field("region", str("eu")), field("tier", i64(2))),
                List.of(field("id", str("x")), field("seq", i64(7)))),
            -6911115321256142798L,
            50,
            2,
            4),
        // Temporal keys (task-184): ts_micros encodes under type tag 5 (canonical epoch micros,
        // 8-byte LE). One identifier-role and one partition-role vector — same numbers as the
        // Rust golden table, so a drift in either side's tag-5 encoding fails a test.
        new Golden(
            "ts_id", key(List.of(), List.of(field("ts", ts(1_782_000_123_456_789L)))), 9199418800307739891L, 243, 3, 3),
        new Golden(
            "ts_part",
            key(
                List.of(field("day", ts(1_782_000_000_000_000L))),
                List.of(field("id", str("doc-1")))),
            3480278431324234352L,
            624,
            0,
            2),
        // Edge cases (task-69): partition strategy with no partition fields falls back to hashing
        // the full key (part8 == hash8), and a fully empty key encodes to empty bytes (fnv offset
        // basis). Same numbers as the Rust golden table.
        new Golden(
            "empty_part", key(List.of(), List.of(field("id", str("solo")))), -107607401798346843L, 933, 5, 5),
        new Golden("empty_key", key(List.of(), List.of()), -3750763034362895579L, 805, 5, 5));
  }

  @Test
  void routeMatchesTheCrossLanguageGoldenVectors() {
    ShardRouter hashed8 = ShardRouter.hashed(8);
    ShardRouter part8 = ShardRouter.partitioned(8);
    // A bucketed router over the balanced(8) map must match legacy hashed(8) — the property
    // (8 divides NUM_BUCKETS) that lets a legacy index adopt buckets without moving any key.
    ShardRouter bucketed8 = ShardRouter.bucketed(ShardRouter.Strategy.HASH, ShardRouter.balancedBucketMap(8));
    for (Golden g : golden()) {
      assertEquals(g.fnv, ShardRouter.fnv1a(ShardRouter.encode(g.key.getPartitionList(), g.key.getIdentifierList())), g.name + ": fnv1a(encode) drifted from Rust");
      assertEquals(g.bucket, hashed8.bucket(g.key), g.name + ": bucket drifted from Rust");
      assertEquals(g.hash8, hashed8.route(g.key), g.name + ": hash routing drifted from Rust");
      assertEquals(g.part8, part8.route(g.key), g.name + ": partition routing drifted from Rust");
      assertEquals(g.hash8, bucketed8.route(g.key), g.name + ": bucketed balanced(8) must match legacy hashed(8)");
    }
  }

  @Test
  void singleShardAlwaysRoutesToZero() {
    assertEquals(0, ShardRouter.hashed(1).route(golden().get(0).key()));
  }

  // --- fan-out (mirrors Rust partition_batch) ---------------------------------

  private static DocOp upsert(Coordinates k) {
    return DocOp.newBuilder()
        .setUpsert(LocatedDoc.newBuilder().setDoc(Document.newBuilder().setKey(k)).build())
        .build();
  }

  @Test
  void partitionPlacesEachOpOnTheShardRoutePicks() {
    ShardRouter router = ShardRouter.hashed(4);
    DocBatch.Builder b =
        DocBatch.newBuilder()
            .setCheckpoint(SourceCheckpoint.newBuilder().setIcebergSnapshot(7).build())
            .setBatchId("batch-A");
    for (int i = 0; i < 30; i++) {
      b.addOps(upsert(key(List.of(), List.of(field("id", str("k" + i))))));
    }
    DocBatch batch = b.build();

    List<DocBatch> parts = ShardedWriteClient.partition(batch, router);
    assertEquals(4, parts.size(), "one sub-batch per shard");

    int total = 0;
    for (int ordinal = 0; ordinal < parts.size(); ordinal++) {
      DocBatch sub = parts.get(ordinal);
      assertEquals("batch-A#s" + ordinal, sub.getBatchId());
      assertEquals(7, sub.getCheckpoint().getIcebergSnapshot());
      total += sub.getOpsCount();
      for (DocOp op : sub.getOpsList()) {
        assertEquals(
            ordinal,
            router.route(op.getUpsert().getDoc().getKey()),
            "op landed on a shard the router would not pick");
      }
    }
    assertEquals(30, total, "every op preserved exactly once across sub-batches");
  }

  @Test
  void partitionPlacesEachOpOnTheBucketMapOwner() {
    // task-77: a bucketed router (from the registry's vended map) fans writes out by bucket owner,
    // exactly as the Gateway routes reads — so write placement matches read routing.
    ShardRouter router = ShardRouter.bucketed(ShardRouter.Strategy.HASH, ShardRouter.balancedBucketMap(4));
    DocBatch.Builder b =
        DocBatch.newBuilder()
            .setCheckpoint(SourceCheckpoint.newBuilder().setIcebergSnapshot(1).build())
            .setBatchId("b");
    for (int i = 0; i < 30; i++) {
      b.addOps(upsert(key(List.of(), List.of(field("id", str("k" + i))))));
    }

    List<DocBatch> parts = ShardedWriteClient.partition(b.build(), router);
    assertEquals(4, parts.size());
    for (int ordinal = 0; ordinal < parts.size(); ordinal++) {
      for (DocOp op : parts.get(ordinal).getOpsList()) {
        assertEquals(
            ordinal,
            router.route(op.getUpsert().getDoc().getKey()),
            "op landed on a shard the bucket map would not pick");
      }
    }
  }

  @Test
  void partitionCoLocatesAndKeepsOrderForDeletesOfAKey() {
    ShardRouter router = ShardRouter.hashed(4);
    Coordinates k = key(List.of(), List.of(field("id", str("doc-1"))));
    DocOp del = DocOp.newBuilder().setDelete(k).build();
    DocBatch batch =
        DocBatch.newBuilder()
            .addOps(upsert(k))
            .addOps(del)
            .setCheckpoint(SourceCheckpoint.newBuilder().setIcebergSnapshot(1).build())
            .setBatchId("b")
            .build();

    List<DocBatch> parts = ShardedWriteClient.partition(batch, router);
    int owner = router.route(k);
    assertEquals(2, parts.get(owner).getOpsCount(), "both ops for a key share a shard");
    assertTrue(parts.get(owner).getOps(0).hasUpsert(), "upsert stays before delete");
    assertTrue(parts.get(owner).getOps(1).hasDelete());

    int elsewhere = 0;
    for (int i = 0; i < parts.size(); i++) {
      if (i != owner) {
        elsewhere += parts.get(i).getOpsCount();
      }
    }
    assertEquals(0, elsewhere, "no other shard received anything");
  }

  @Test
  void partitionCopiesResumeFloorAndFromOntoEverySubBatch() {
    // task-204 + task-194: the connector stamps the window's `from` and its resume FLOOR on the
    // top-level batch; partition must copy BOTH onto every sub-batch so each shard's continuity
    // guard and idempotency prune see the same source positions.
    ShardRouter router = ShardRouter.hashed(3);
    DocBatch.Builder b =
        DocBatch.newBuilder()
            .setCheckpoint(SourceCheckpoint.newBuilder().setIcebergSnapshot(9).build())
            .setBatchId("b")
            .setFromCheckpoint(SourceCheckpoint.newBuilder().setIcebergSnapshot(4).build())
            .setSafeCheckpoint(SourceCheckpoint.newBuilder().setIcebergSnapshot(4).build());
    for (int i = 0; i < 12; i++) {
      b.addOps(upsert(key(List.of(), List.of(field("id", str("k" + i))))));
    }

    List<DocBatch> parts = ShardedWriteClient.partition(b.build(), router);
    assertEquals(3, parts.size());
    for (DocBatch sub : parts) {
      assertTrue(sub.hasSafeCheckpoint(), "sub-batch carries the resume floor");
      assertEquals(4, sub.getSafeCheckpoint().getIcebergSnapshot());
      assertTrue(sub.hasFromCheckpoint(), "sub-batch carries the window from");
      assertEquals(4, sub.getFromCheckpoint().getIcebergSnapshot());
    }
  }

  @Test
  void partitionOmitsResumeFloorWhenAbsent() {
    // No floor on the top-level batch (e.g. an empty shard set the connector never resumed past) →
    // no sub-batch invents one, so the Node prunes nothing (task-204).
    ShardRouter router = ShardRouter.hashed(2);
    DocBatch batch =
        DocBatch.newBuilder()
            .addOps(upsert(key(List.of(), List.of(field("id", str("x"))))))
            .setCheckpoint(SourceCheckpoint.newBuilder().setIcebergSnapshot(1).build())
            .setBatchId("b")
            .build();

    for (DocBatch sub : ShardedWriteClient.partition(batch, router)) {
      assertTrue(!sub.hasSafeCheckpoint(), "no floor is invented when the batch carries none");
    }
  }
}
