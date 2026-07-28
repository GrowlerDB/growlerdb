package io.growlerdb.connector;

import io.growlerdb.proto.v1.ControlPlaneGrpc;
import io.growlerdb.proto.v1.GetIndexRequest;
import io.growlerdb.proto.v1.GetIndexResponse;
import io.growlerdb.proto.v1.ResolveUnitOwnerRequest;
import io.growlerdb.proto.v1.ResolveUnitOwnerResponse;
import io.grpc.stub.StreamObserver;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test double of the ControlPlane service with <b>mutable placement</b>: tests flip the index's
 * shard map ({@link #setIndex}) or the windows' owning endpoint ({@link #setWindowOwner}) mid-test
 * to model a CP re-placement, and assert the connector's clients re-resolve onto the new owner
 * instead of staying pinned to the startup snapshot.
 */
final class FakeControlPlane extends ControlPlaneGrpc.ControlPlaneImplBase {

  private volatile GetIndexResponse index = GetIndexResponse.getDefaultInstance();
  /** The endpoint resolveUnitOwner hands out for EVERY window (one owner is enough for the tests). */
  private volatile String windowOwner = "";
  /** Per-call owner overrides, consumed in order before {@link #windowOwner} — so a test can model
   *  "first resolve says A (dead), the re-resolve after its failure says B (the re-placement)". */
  private final Deque<String> ownerQueue = new ConcurrentLinkedDeque<>();

  final AtomicInteger getIndexCalls = new AtomicInteger();
  final AtomicInteger resolveCalls = new AtomicInteger();

  void setIndex(GetIndexResponse index) {
    this.index = index;
  }

  void setWindowOwner(String endpoint) {
    this.windowOwner = endpoint;
  }

  void queueWindowOwner(String endpoint) {
    ownerQueue.addLast(endpoint);
  }

  @Override
  public void getIndex(GetIndexRequest request, StreamObserver<GetIndexResponse> obs) {
    getIndexCalls.incrementAndGet();
    obs.onNext(index);
    obs.onCompleted();
  }

  @Override
  public void resolveUnitOwner(
      ResolveUnitOwnerRequest request, StreamObserver<ResolveUnitOwnerResponse> obs) {
    resolveCalls.incrementAndGet();
    String queued = ownerQueue.pollFirst();
    obs.onNext(
        ResolveUnitOwnerResponse.newBuilder()
            .setEndpoint(queued != null ? queued : windowOwner)
            .build());
    obs.onCompleted();
  }
}
