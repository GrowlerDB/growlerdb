package io.growlerdb.connector;

import io.growlerdb.proto.v1.DocBatch;
import io.growlerdb.proto.v1.DocOp;
import io.growlerdb.proto.v1.GetCheckpointRequest;
import io.growlerdb.proto.v1.GetCheckpointResponse;
import io.growlerdb.proto.v1.SourceCheckpoint;
import io.growlerdb.proto.v1.WriteGrpc;
import io.growlerdb.proto.v1.WriteRequest;
import io.growlerdb.proto.v1.WriteResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test double of one Node shard's Write service that ENFORCES the ingest contract — unlike
 * {@code RecordingWrite}, which records blindly. Mirrors the Rust {@code Shard::continuity}
 * (task-196 window-covering by sequence number, exact-match fallback without one), batch-id
 * dedup, the safe-checkpoint prune (so tests can prove the set survives WITHOUT dedup records),
 * and {@code GetCheckpoint} with the stored sequence. Used by the connector-set tests to catch
 * a guard violation as a loud {@code FAILED_PRECONDITION}, exactly like the real node.
 */
final class FakeShardNode extends WriteGrpc.WriteImplBase {

  private record Cp(long id, long seq) {}

  private Cp current; // null = no checkpoint yet
  private long snapshot;
  /** batch_id → the end-checkpoint sequence it committed at (for the prune). */
  final Map<String, Long> batchKeys = new ConcurrentHashMap<>();
  /** Last-write-wins doc state by identifier string ("" body = deleted marker removed). */
  final Map<String, DocOp> applied = new ConcurrentHashMap<>();
  final List<DocBatch> received = new CopyOnWriteArrayList<>();
  final AtomicInteger gaps = new AtomicInteger();

  @Override
  public synchronized void write(WriteRequest request, StreamObserver<WriteResponse> obs) {
    DocBatch batch = request.getBatch();
    received.add(batch);
    if (batchKeys.containsKey(batch.getBatchId())) {
      // Idempotent replay by id — no re-apply, no advance.
      obs.onNext(WriteResponse.newBuilder().setSnapshot(snapshot).build());
      obs.onCompleted();
      return;
    }
    switch (continuity(batch)) {
      case GAP -> {
        gaps.incrementAndGet();
        obs.onError(
            Status.FAILED_PRECONDITION
                .withDescription(
                    "CHECKPOINT_GAP: batch " + batch.getBatchId() + " does not cover " + current)
                .asRuntimeException());
        return;
      }
      case NO_OP -> {
        // Ends at/behind this shard: nothing to apply, never a regression.
        obs.onNext(WriteResponse.newBuilder().setSnapshot(snapshot).build());
        obs.onCompleted();
        return;
      }
      case APPLY -> {
        for (DocOp op : batch.getOpsList()) {
          if (op.hasDelete()) {
            applied.remove(identifierOf(op));
          } else {
            applied.put(identifierOf(op), op);
          }
        }
        SourceCheckpoint end = batch.getCheckpoint();
        current = new Cp(end.getIcebergSnapshot(), end.getIcebergSequenceNumber());
        snapshot++;
        batchKeys.put(batch.getBatchId(), end.getIcebergSequenceNumber());
        if (batch.hasSafeCheckpoint()) {
          long floor = batch.getSafeCheckpoint().getIcebergSequenceNumber();
          if (floor > 0) {
            batchKeys.values().removeIf(seq -> seq > 0 && seq <= floor);
          }
        }
        obs.onNext(WriteResponse.newBuilder().setSnapshot(snapshot).build());
        obs.onCompleted();
      }
    }
  }

  @Override
  public synchronized void getCheckpoint(
      GetCheckpointRequest request, StreamObserver<GetCheckpointResponse> obs) {
    GetCheckpointResponse.Builder response = GetCheckpointResponse.newBuilder().setSnapshot(snapshot);
    if (current != null) {
      SourceCheckpoint.Builder cp = SourceCheckpoint.newBuilder().setIcebergSnapshot(current.id());
      if (current.seq() > 0) {
        cp.setIcebergSequenceNumber(current.seq());
      }
      response.setCheckpoint(cp);
    }
    obs.onNext(response.build());
    obs.onCompleted();
  }

  synchronized Long checkpointSnapshotId() {
    return current == null ? null : current.id();
  }

  private enum Decision {
    APPLY,
    NO_OP,
    GAP
  }

  /** Mirror of the Rust {@code Shard::continuity} decision (store.rs, task-196). */
  private Decision continuity(DocBatch batch) {
    if (current == null) {
      return Decision.APPLY;
    }
    SourceCheckpoint end = batch.getCheckpoint();
    boolean samePosition = end.getIcebergSnapshot() == current.id();
    long endSeq = end.getIcebergSequenceNumber();
    boolean ordered = endSeq > 0 && current.seq() > 0;
    if (samePosition) {
      return batch.hasFromCheckpoint() ? Decision.NO_OP : Decision.APPLY;
    }
    if (ordered) {
      if (endSeq <= current.seq()) {
        return Decision.NO_OP;
      }
      if (!batch.hasFromCheckpoint()) {
        return Decision.APPLY;
      }
      long fromSeq = batch.getFromCheckpoint().getIcebergSequenceNumber();
      boolean fromAtCurrent = batch.getFromCheckpoint().getIcebergSnapshot() == current.id();
      if (fromAtCurrent) {
        return Decision.APPLY;
      }
      return (fromSeq > 0 && fromSeq <= current.seq()) ? Decision.APPLY : Decision.GAP;
    }
    // Legacy exact-match fallback.
    if (!batch.hasFromCheckpoint()) {
      return Decision.APPLY;
    }
    return batch.getFromCheckpoint().getIcebergSnapshot() == current.id()
        ? Decision.APPLY
        : Decision.GAP;
  }

  private static String identifierOf(DocOp op) {
    return op.hasUpsert()
        ? op.getUpsert().getDoc().getKey().getIdentifier(0).getValue().getStr()
        : op.getDelete().getIdentifier(0).getValue().getStr();
  }
}
