package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.growlerdb.proto.v1.Coordinates;
import io.growlerdb.proto.v1.DocBatch;
import io.growlerdb.proto.v1.DocOp;
import io.growlerdb.proto.v1.Document;
import io.growlerdb.proto.v1.Field;
import io.growlerdb.proto.v1.LocatedDoc;
import io.growlerdb.proto.v1.SourceCheckpoint;
import io.growlerdb.proto.v1.Value;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Finding 1 (PR #294 review). A <b>changelog</b> (delete-aware) reindex promotes the new generation at
 * the <b>build snapshot</b>, so on cutover the shard's committed checkpoint rewinds beneath a live
 * streaming connector's in-memory cursor. This proves that rewind is <b>self-healing</b>, not the fatal
 * crash the review described:
 *
 * <ol>
 *   <li>the connector's next micro-batch (resuming from the pre-cutover live head) trips exactly one
 *       {@code CheckpointGap} — the <i>intended</i> loud signal, because a genuinely missed write is
 *       indistinguishable from this rewind at the node, so silently accepting {@code from > current}
 *       would seal a real loss;</li>
 *   <li>the connector's restart loop re-reads its resume point from the node's committed checkpoint
 *       (now the build snapshot) via {@code GetCheckpoint};</li>
 *   <li>replaying the build-window delta from there applies exactly-once ({@code from <= current =>
 *       Apply}), and a re-sent batch is deduped by {@code batch_id}.</li>
 * </ol>
 *
 * <p>Spark's restart mechanics (StreamingQueryException &rarr; re-resolve &rarr; resume) are covered by
 * {@link PlacementRefreshTest} and {@code ConnectorMetricsTest}; this isolates the <i>checkpoint</i>
 * self-heal that makes the restart safe, against a {@link FakeShardNode} that mirrors the Rust
 * {@code Shard::continuity} guard. No Spark.
 */
class ChangelogCutoverSelfHealTest {

  private final List<Server> servers = new ArrayList<>();

  @AfterEach
  void tearDown() {
    servers.forEach(Server::shutdownNow);
  }

  private Server start(io.grpc.BindableService service) throws IOException {
    Server server = ServerBuilder.forPort(0).addService(service).build().start();
    servers.add(server);
    return server;
  }

  /** An ordered (lineage-stamped) checkpoint: snapshot id + monotone Iceberg sequence number. */
  private static SourceCheckpoint cp(long snapshot, long seq) {
    return SourceCheckpoint.newBuilder()
        .setIcebergSnapshot(snapshot)
        .setIcebergSequenceNumber(seq)
        .build();
  }

  private static DocOp upsert(String id) {
    Coordinates key =
        Coordinates.newBuilder()
            .addIdentifier(Field.newBuilder().setName("id").setValue(Value.newBuilder().setStr(id)))
            .build();
    Document doc =
        Document.newBuilder().setKey(key).putFields("id", Value.newBuilder().setStr(id).build()).build();
    return DocOp.newBuilder().setUpsert(LocatedDoc.newBuilder().setDoc(doc)).build();
  }

  /** {@code from == null} is a bootstrap batch (start of the changelog / a fresh build). */
  private static DocBatch batch(String id, SourceCheckpoint from, SourceCheckpoint to, DocOp... ops) {
    DocBatch.Builder b =
        DocBatch.newBuilder().setBatchId(id).setCheckpoint(to).addAllOps(java.util.Arrays.asList(ops));
    if (from != null) {
      b.setFromCheckpoint(from);
    }
    return b.build();
  }

  @Test
  void aChangelogCutoverRewindSelfHealsWithoutManualRecovery() throws IOException, InterruptedException {
    // The build snapshot the reindex promoted at; the live head the connector's cursor reached while
    // the (unfenced) build ran; and the head its next micro-batch would advance to.
    SourceCheckpoint buildSnap = cp(100, 5);
    SourceCheckpoint liveHead = cp(200, 9);
    SourceCheckpoint newHead = cp(300, 12);

    FakeShardNode node = new FakeShardNode();
    Server server = start(node);
    // maxAttempts = 1: the gap is non-retryable and must surface immediately, not churn on backoff.
    WriteClient client = new WriteClient("127.0.0.1", server.getPort(), 5, 1, 10, 20);
    try {
      // Post-cutover on-disk state: the promoted generation is stamped at the BUILD SNAPSHOT (a fresh
      // build, so from = null). This IS the checkpoint regression a changelog cutover produces.
      long afterBuild = client.write(batch("reindex-build@100", null, buildSnap, upsert("a")));
      assertEquals(
          buildSnap.getIcebergSnapshot(),
          client.checkpointSnapshotId(),
          "the shard now serves the new generation stamped at the build snapshot");

      // (1) The live streaming connector still holds an in-memory cursor at the pre-cutover live head,
      // so its next micro-batch resumes `from` there — ahead of the rewound checkpoint. The node
      // refuses it with a LOUD CheckpointGap rather than silently sealing a possible missed write.
      StatusRuntimeException gap =
          assertThrows(
              StatusRuntimeException.class,
              () -> client.write(batch("live->new", liveHead, newHead, upsert("b"))));
      assertEquals(Status.Code.FAILED_PRECONDITION, gap.getStatus().getCode());
      assertEquals(1, node.gaps.get(), "exactly one gap — the cutover rewind");
      assertEquals(
          buildSnap.getIcebergSnapshot(),
          client.checkpointSnapshotId(),
          "the refused write left the checkpoint untouched");
      assertFalse(node.applied.containsKey("b"), "the gapped batch applied nothing");

      // (2) Restart loop: exactly-once rests on the NODE's checkpoint, so the connector re-reads its
      // resume point from there (now the build snapshot) instead of its stale in-memory cursor.
      WriteClient.ShardCheckpoint resume = client.checkpoint();
      assertEquals(buildSnap.getIcebergSnapshot(), resume.snapshotId());
      assertEquals(buildSnap.getIcebergSequenceNumber(), resume.sequenceNumber().orElseThrow());

      // (3) Replay the build-window delta FROM the node's checkpoint — `from == current` => Apply. This
      // is the connector's normal delete-aware changelog replay, resumed from the rewound point.
      SourceCheckpoint from = cp(resume.snapshotId(), resume.sequenceNumber().orElseThrow());
      long afterReplay = client.write(batch("build->new", from, newHead, upsert("b")));
      assertEquals(
          newHead.getIcebergSnapshot(),
          client.checkpointSnapshotId(),
          "the replay advanced the shard to head");
      assertTrue(node.applied.containsKey("b"), "the build-window delta landed");
      assertEquals(1, node.gaps.get(), "no further gaps once resumed from the checkpoint");
      assertTrue(afterReplay > afterBuild, "the replay committed a new snapshot");

      // Exactly-once across the restart: re-sending the same replay batch is deduped by batch_id — no
      // double-apply, no second checkpoint advance.
      long resent = client.write(batch("build->new", from, newHead, upsert("b")));
      assertEquals(afterReplay, resent, "a batch_id replay is a no-op: the checkpoint doesn't move");
    } finally {
      try {
        client.close();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
