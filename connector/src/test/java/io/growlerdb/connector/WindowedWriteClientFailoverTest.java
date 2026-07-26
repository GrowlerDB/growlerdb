package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.growlerdb.proto.v1.Coordinates;
import io.growlerdb.proto.v1.DocBatch;
import io.growlerdb.proto.v1.DocOp;
import io.growlerdb.proto.v1.Document;
import io.growlerdb.proto.v1.Field;
import io.growlerdb.proto.v1.LocatedDoc;
import io.growlerdb.proto.v1.SourceCheckpoint;
import io.growlerdb.proto.v1.Value;
import io.growlerdb.proto.v1.WindowingConfig;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * HA-E1 (windowed path): the window → owner pin is a cache with <b>invalidation</b>, not a
 * process-lifetime binding. A transport-class write failure drops the pin and re-resolves the
 * owner from the CP — in place when the CP has already re-placed the window, or on the next batch
 * otherwise. An application error keeps the pin (re-resolving can't fix a rejected batch).
 */
class WindowedWriteClientFailoverTest {

  private static final long DAY = 86_400_000_000L;
  private static final String INDEX = "movies";
  /** Nothing listens here — dials fail fast with UNAVAILABLE (connection refused). */
  private static final String DEAD_ENDPOINT = "127.0.0.1:1";

  private final List<Server> servers = new ArrayList<>();
  private final List<ControlPlaneClient> cps = new ArrayList<>();

  @AfterEach
  void tearDown() throws InterruptedException {
    for (ControlPlaneClient cp : cps) {
      cp.close();
    }
    servers.forEach(Server::shutdownNow);
  }

  private Server start(io.grpc.BindableService service) throws IOException {
    Server server = ServerBuilder.forPort(0).addService(service).build().start();
    servers.add(server);
    return server;
  }

  private ControlPlaneClient cpClientFor(FakeControlPlane service) throws IOException {
    ControlPlaneClient cp = new ControlPlaneClient("127.0.0.1", start(service).getPort());
    cps.add(cp);
    return cp;
  }

  /** A windowed writer whose dialed WriteClients fail fast: 2s deadline, ONE attempt, no backoff. */
  private static WindowedWriteClient writerOver(ControlPlaneClient cp) {
    WindowingConfig windowing =
        WindowingConfig.newBuilder()
            .setField("ts")
            .setGranularity("daily")
            .setFieldFormat("epoch_micros")
            .build();
    return new WindowedWriteClient(
        INDEX,
        cp,
        windowing,
        SnapshotLineage.none(),
        endpoint -> {
          String[] hp = endpoint.replaceFirst("^https?://", "").split(":", 2);
          return new WriteClient(hp[0].trim(), Integer.parseInt(hp[1].trim()), INDEX, 2, 1, 10, 10);
        });
  }

  private static DocBatch dayTenBatch(String batchId) {
    Coordinates key =
        Coordinates.newBuilder()
            .addIdentifier(Field.newBuilder().setName("id").setValue(Value.newBuilder().setStr("a")))
            .build();
    Document doc =
        Document.newBuilder()
            .setKey(key)
            .putFields("id", Value.newBuilder().setStr("a").build())
            .putFields("ts", Value.newBuilder().setInt(10 * DAY).build())
            .build();
    return DocBatch.newBuilder()
        .addOps(DocOp.newBuilder().setUpsert(LocatedDoc.newBuilder().setDoc(doc)))
        .setCheckpoint(SourceCheckpoint.newBuilder().setIcebergSnapshot(7).setIcebergSequenceNumber(1))
        .setBatchId(batchId)
        .build();
  }

  /** First resolve → dead A; the failure drops the pin, the SAME write re-resolves onto live B. */
  @Test
  void aStaleWindowPinReResolvesInPlaceOntoTheRePlacedOwner()
      throws IOException, InterruptedException {
    FakeShardNode nodeB = new FakeShardNode();
    String endpointB = "127.0.0.1:" + start(nodeB).getPort();
    FakeControlPlane cpService = new FakeControlPlane();
    cpService.queueWindowOwner(DEAD_ENDPOINT); // the stale placement, first ask only
    cpService.setWindowOwner(endpointB); // the CP has re-placed the window
    WindowedWriteClient client = writerOver(cpClientFor(cpService));
    try {
      client.write(dayTenBatch("b1"));
    } finally {
      client.close();
    }
    assertEquals(2, cpService.resolveCalls.get(), "failed pin → exactly one re-resolve");
    assertEquals(1, nodeB.received.size(), "the write recovered onto the new owner");
    assertEquals(INDEX, nodeB.writeRequests.get(0).getIndex(), "still index-tagged");
  }

  /** Both attempts dead: the error propagates, but the pin is DROPPED — the next batch (the
   *  restart-loop replay) re-resolves and lands on the moved owner instead of staying wedged. */
  @Test
  void aFailedWindowIsUnpinnedSoTheNextBatchFollowsTheMove()
      throws IOException, InterruptedException {
    FakeShardNode nodeB = new FakeShardNode();
    String endpointB = "127.0.0.1:" + start(nodeB).getPort();
    FakeControlPlane cpService = new FakeControlPlane();
    cpService.setWindowOwner(DEAD_ENDPOINT); // the CP hasn't re-placed yet
    WindowedWriteClient client = writerOver(cpClientFor(cpService));
    try {
      StatusRuntimeException e =
          assertThrows(StatusRuntimeException.class, () -> client.write(dayTenBatch("b1")));
      assertEquals(Status.Code.UNAVAILABLE, e.getStatus().getCode());
      assertEquals(2, cpService.resolveCalls.get(), "resolved, failed, re-resolved, failed");

      // The CP re-places the window; the replayed batch (idempotent batch_id) must follow it.
      cpService.setWindowOwner(endpointB);
      client.write(dayTenBatch("b1"));
      assertEquals(1, nodeB.received.size(), "replay landed on the re-placed owner");
    } finally {
      client.close();
    }
  }

  /** An application rejection keeps the pin: re-resolving placement can't fix a bad batch. */
  @Test
  void anApplicationErrorDoesNotReResolvePlacement() throws IOException, InterruptedException {
    RejectingNode rejecting = new RejectingNode();
    String endpoint = "127.0.0.1:" + start(rejecting).getPort();
    FakeControlPlane cpService = new FakeControlPlane();
    cpService.setWindowOwner(endpoint);
    WindowedWriteClient client = writerOver(cpClientFor(cpService));
    try {
      StatusRuntimeException e =
          assertThrows(StatusRuntimeException.class, () -> client.write(dayTenBatch("b1")));
      assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
      assertEquals(1, cpService.resolveCalls.get(), "no placement re-resolve on a rejected batch");
      assertEquals(1, rejecting.calls.get());
    } finally {
      client.close();
    }
  }

  /** Always rejects with a non-transport application error. */
  private static final class RejectingNode extends io.growlerdb.proto.v1.WriteGrpc.WriteImplBase {
    final AtomicInteger calls = new AtomicInteger();

    @Override
    public void write(
        io.growlerdb.proto.v1.WriteRequest request,
        StreamObserver<io.growlerdb.proto.v1.WriteResponse> obs) {
      calls.incrementAndGet();
      obs.onError(Status.INVALID_ARGUMENT.withDescription("bad batch").asRuntimeException());
    }
  }
}
