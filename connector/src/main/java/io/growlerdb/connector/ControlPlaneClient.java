package io.growlerdb.connector;

import io.growlerdb.proto.v1.ControlPlaneGrpc;
import io.growlerdb.proto.v1.GetIndexRequest;
import io.growlerdb.proto.v1.GetIndexResponse;
import io.growlerdb.proto.v1.ResolveWindowOwnerRequest;
import io.growlerdb.proto.v1.ResolveWindowOwnerResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.TimeUnit;

/**
 * Thin client to the GrowlerDB {@code ControlPlane} gRPC service. The connector uses it
 * to fetch an index's <b>routing config</b> from the registry — the same source the Gateway reads
 * from — so write placement and read routing can't drift. {@link #getIndex} returns the
 * shard count and routing strategy; the connector validates its own endpoint set against them and
 * fails fast on a mismatch instead of silently writing where reads never look.
 */
public final class ControlPlaneClient implements AutoCloseable {

  private final ManagedChannel channel;
  private final ControlPlaneGrpc.ControlPlaneBlockingStub stub;

  /** Connect to the Control Plane at {@code host:port} (plaintext; TLS/auth not yet supported). */
  public ControlPlaneClient(String host, int port) {
    this.channel = ManagedChannelBuilder.forTarget("dns:///" + host + ":" + port).usePlaintext().build();
    this.stub = ControlPlaneGrpc.newBlockingStub(channel);
  }

  /** The registry's routing config for {@code index} (shard count + strategy, + windowing config). */
  public GetIndexResponse getIndex(String index) {
    return stub.getIndex(GetIndexRequest.newBuilder().setName(index).build());
  }

  /**
   * Resolve the node that owns a time {@code window} of a windowed {@code index}, placing
   * it on the least-loaded live node on first ask. The connector calls this with each row's computed
   * window id to learn where to stream that window's writes. Idempotent for an already-placed window.
   */
  public ResolveWindowOwnerResponse resolveWindowOwner(String index, long window) {
    return stub.resolveWindowOwner(
        ResolveWindowOwnerRequest.newBuilder().setIndex(index).setWindow(window).build());
  }

  @Override
  public void close() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }
}
