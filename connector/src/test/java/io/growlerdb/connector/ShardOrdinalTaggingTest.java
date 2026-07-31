package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.growlerdb.proto.v1.DocBatch;
import io.growlerdb.proto.v1.GetCheckpointRequest;
import io.growlerdb.proto.v1.GetCheckpointResponse;
import io.growlerdb.proto.v1.WriteGrpc;
import io.growlerdb.proto.v1.WriteRequest;
import io.growlerdb.proto.v1.WriteResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * A hash pool node (D52) dispatches by {@code (index, shard)}: an ordinal-tagged {@link WriteClient}
 * must stamp its ordinal on every {@code Write} and {@code GetCheckpoint}. These assert it lands on the wire.
 */
class ShardOrdinalTaggingTest {

  /** An ordinal-tagged writer stamps its ordinal on the write AND the resume checkpoint. */
  @Test
  void writeAndCheckpointCarryTheOrdinalShardSelector() throws IOException, InterruptedException {
    CapturingWrite handler = new CapturingWrite();
    Server server = ServerBuilder.forPort(0).addService(handler).build().start();
    try (WriteClient client = new WriteClient("127.0.0.1", server.getPort(), "docs", 3)) {
      client.write(DocBatch.newBuilder().setBatchId("b1").build());
      assertEquals("docs", handler.write.get().getIndex());
      assertEquals(3, handler.write.get().getShard(), "write carries its ordinal");

      client.checkpoint(0L);
      assertEquals("docs", handler.checkpoint.get().getIndex());
      assertEquals(3, handler.checkpoint.get().getShard(), "resume asks the same ordinal");
    } finally {
      server.shutdownNow();
    }
  }

  /** A windowed / single-shard writer leaves the shard selector at 0 (those nodes ignore it). */
  @Test
  void anUntaggedWriterLeavesTheShardSelectorZero() throws IOException, InterruptedException {
    CapturingWrite handler = new CapturingWrite();
    Server server = ServerBuilder.forPort(0).addService(handler).build().start();
    try (WriteClient client = new WriteClient("127.0.0.1", server.getPort(), "docs")) {
      client.write(DocBatch.newBuilder().setBatchId("b1").build());
      assertEquals(0, handler.write.get().getShard());
    } finally {
      server.shutdownNow();
    }
  }

  /** Records the last Write / GetCheckpoint request, acking each so the client proceeds. */
  private static final class CapturingWrite extends WriteGrpc.WriteImplBase {
    final AtomicReference<WriteRequest> write = new AtomicReference<>();
    final AtomicReference<GetCheckpointRequest> checkpoint = new AtomicReference<>();

    @Override
    public void write(WriteRequest request, StreamObserver<WriteResponse> obs) {
      write.set(request);
      obs.onNext(WriteResponse.newBuilder().setSnapshot(1).build());
      obs.onCompleted();
    }

    @Override
    public void getCheckpoint(
        GetCheckpointRequest request, StreamObserver<GetCheckpointResponse> obs) {
      checkpoint.set(request);
      obs.onNext(GetCheckpointResponse.newBuilder().setSnapshot(1).build());
      obs.onCompleted();
    }
  }
}
