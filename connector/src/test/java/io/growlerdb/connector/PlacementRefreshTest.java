package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.growlerdb.proto.v1.DocBatch;
import io.growlerdb.proto.v1.GetIndexResponse;
import io.growlerdb.proto.v1.RoutingStrategy;
import io.growlerdb.proto.v1.ShardStatus;
import io.growlerdb.proto.v1.SourceCheckpoint;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * HA-E1 (hash path): placement must be re-resolved from the control plane on every stream restart,
 * not once per process — so after a CP re-placement the connector recovers onto the new primary
 * instead of hammering the deposed endpoint (or silently committing to an alive-but-deposed node).
 */
class PlacementRefreshTest {

  private final List<Server> servers = new ArrayList<>();

  @AfterEach
  void tearDown() {
    servers.forEach(Server::shutdownNow);
  }

  private <T extends io.grpc.BindableService> Server start(T service) throws IOException {
    Server server = ServerBuilder.forPort(0).addService(service).build().start();
    servers.add(server);
    return server;
  }

  private static GetIndexResponse oneShardOn(String endpoint) {
    return GetIndexResponse.newBuilder()
        .setShardCount(1)
        .setRouting(RoutingStrategy.ROUTING_HASH)
        .addShardStatus(ShardStatus.newBuilder().setOrdinal(0).setPrimary(endpoint))
        .build();
  }

  private static DocBatch batch(String id, long snapshot, long seq) {
    return DocBatch.newBuilder()
        .setBatchId(id)
        .setCheckpoint(
            SourceCheckpoint.newBuilder().setIcebergSnapshot(snapshot).setIcebergSequenceNumber(seq))
        .build();
  }

  /** Endpoint A at startup, CP re-places onto B, the rebuilt writer lands on B — tagged. */
  @Test
  void rebuiltWriterFollowsACpRePlacement() throws IOException, InterruptedException {
    FakeShardNode nodeA = new FakeShardNode();
    FakeShardNode nodeB = new FakeShardNode();
    String endpointA = "127.0.0.1:" + start(nodeA).getPort();
    String endpointB = "127.0.0.1:" + start(nodeB).getPort();
    FakeControlPlane cpService = new FakeControlPlane();
    cpService.setIndex(oneShardOn(endpointA));
    ControlPlaneClient cp = new ControlPlaneClient("127.0.0.1", start(cpService).getPort());
    try {
      var factory =
          ConnectorApp.cpResolvedWriterFactory(
              cp, "movies", ShardRouter.Strategy.HASH, null, SnapshotLineage.none(), null, null);

      BatchWriter writer = factory.get();
      writer.write(batch("b1", 7, 1));
      assertEquals(1, nodeA.received.size(), "startup placement: writes land on A");
      assertEquals(0, nodeB.received.size());

      // The CP re-places shard 0 onto B (A crashed, or was deposed). A restart rebuilds through
      // the factory and must dial B — the old writer would keep committing to A forever.
      cpService.setIndex(oneShardOn(endpointB));
      BatchWriter rebuilt = ConnectorApp.rebuildWriter(writer, factory);
      assertNotSame(writer, rebuilt, "a successful re-resolution swaps the writer");
      try {
        rebuilt.write(batch("b2", 8, 2));
      } finally {
        rebuilt.close();
      }
      assertEquals(1, nodeA.received.size(), "the deposed node gets nothing more");
      assertEquals(1, nodeB.received.size(), "recovered onto the re-placed owner");
      assertEquals("movies", nodeB.writeRequests.get(0).getIndex(), "still index-tagged");
      assertTrue(cpService.getIndexCalls.get() >= 2, "each factory call re-asks the CP");
    } finally {
      cp.close();
    }
  }

  /** A failed re-resolution (CP down mid-failover) keeps the current writer instead of dying. */
  @Test
  void rebuildKeepsTheCurrentWriterWhenResolutionFails() {
    AtomicBoolean closed = new AtomicBoolean();
    BatchWriter current =
        new BatchWriter() {
          @Override
          public long write(DocBatch batch) {
            return 0;
          }

          @Override
          public Long checkpointSnapshotId() {
            return null;
          }

          @Override
          public boolean drainedTo(long head) {
            return false;
          }

          @Override
          public void close() {
            closed.set(true);
          }
        };
    BatchWriter kept =
        ConnectorApp.rebuildWriter(
            current,
            () -> {
              throw new IllegalStateException("control plane unreachable");
            });
    assertSame(current, kept, "resolution failure degrades to retry-in-place");
    assertTrue(!closed.get(), "the kept writer must not be closed under it");
  }
}
