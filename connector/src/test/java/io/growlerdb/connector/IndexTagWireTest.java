package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.growlerdb.proto.v1.DocBatch;
import io.growlerdb.proto.v1.GetCheckpointRequest;
import io.growlerdb.proto.v1.SourceCheckpoint;
import io.growlerdb.proto.v1.WriteRequest;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Wire-level assertions that the {@code index} selector reaches the Node on EVERY tagged path —
 * writes AND the resume/drain checkpoints (HA-E2). A pool node serving several indexes rejects an
 * empty selector as a non-retryable {@code InvalidArgument}, so an untagged {@code GetCheckpoint}
 * on the resume path is a guaranteed crash-loop against a multi-index node: writes land tagged but
 * the very next restart dies asking "whose checkpoint?".
 */
class IndexTagWireTest {

  private static final String INDEX = "movies";

  private final List<Server> servers = new ArrayList<>();
  private final List<FakeShardNode> nodes = new ArrayList<>();

  @AfterEach
  void tearDown() {
    servers.forEach(Server::shutdownNow);
  }

  private List<String> startNodes(int n) throws IOException {
    List<String> endpoints = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      FakeShardNode node = new FakeShardNode();
      Server server = ServerBuilder.forPort(0).addService(node).build().start();
      nodes.add(node);
      servers.add(server);
      endpoints.add("127.0.0.1:" + server.getPort());
    }
    return endpoints;
  }

  private static DocBatch batch(String id, long snapshot, long seq) {
    return DocBatch.newBuilder()
        .setBatchId(id)
        .setCheckpoint(
            SourceCheckpoint.newBuilder().setIcebergSnapshot(snapshot).setIcebergSequenceNumber(seq))
        .build();
  }

  private void assertAllTagged() {
    for (FakeShardNode node : nodes) {
      assertFalse(node.writeRequests.isEmpty() && node.checkpointRequests.isEmpty());
      for (WriteRequest w : node.writeRequests) {
        assertEquals(INDEX, w.getIndex(), "WriteRequest.index must carry the tag");
      }
      for (GetCheckpointRequest c : node.checkpointRequests) {
        assertEquals(INDEX, c.getIndex(), "GetCheckpointRequest.index must carry the tag");
      }
    }
  }

  @Test
  void shardedWritesResumeAndDrainAllCarryTheIndexTag() throws IOException, InterruptedException {
    List<String> endpoints = startNodes(2);
    ShardedWriteClient client =
        new ShardedWriteClient(
            endpoints,
            new ShardRouter(2, ShardRouter.Strategy.HASH),
            SnapshotLineage.none(),
            INDEX);
    try {
      client.write(batch("b1", 7, 1)); // empty sub-batches still land on every shard
      assertEquals(7L, client.checkpointSnapshotId()); // resumeMin → GetCheckpoint per shard
      assertTrue(client.drainedTo(7)); // drain barrier → GetCheckpoint per shard
    } finally {
      client.close();
    }
    assertAllTagged();
    for (FakeShardNode node : nodes) {
      assertTrue(node.checkpointRequests.size() >= 2, "resume AND drain both asked, tagged");
    }
  }

  @Test
  void shardGroupWritesAndResumeCarryTheIndexTag() throws IOException, InterruptedException {
    List<String> endpoints = startNodes(2);
    ShardGroupWriteClient client =
        new ShardGroupWriteClient(
            endpoints,
            new ShardRouter(2, ShardRouter.Strategy.HASH),
            SnapshotLineage.none(),
            new TreeSet<>(List.of(0, 1)),
            INDEX);
    try {
      client.write(batch("b1", 7, 1));
      assertEquals(7L, client.checkpointSnapshotId());
      assertTrue(client.drainedTo(7));
    } finally {
      client.close();
    }
    assertAllTagged();
  }

  /** The single-endpoint path must carry the tag too — it used to hand back a bare untagged client. */
  @Test
  void singleEndpointWriterCarriesTheIndexTag() throws IOException, InterruptedException {
    List<String> endpoints = startNodes(1);
    BatchWriter client =
        ConnectorApp.writerFor(
            endpoints, ShardRouter.Strategy.HASH, null, SnapshotLineage.none(), INDEX);
    try {
      client.write(batch("b1", 7, 1));
      assertEquals(7L, client.checkpointSnapshotId());
      assertTrue(client.drainedTo(7));
    } finally {
      client.close();
    }
    assertAllTagged();
  }
}
