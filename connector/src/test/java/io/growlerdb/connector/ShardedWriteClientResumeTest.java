package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.growlerdb.proto.v1.GetCheckpointRequest;
import io.growlerdb.proto.v1.GetCheckpointResponse;
import io.growlerdb.proto.v1.SourceCheckpoint;
import io.growlerdb.proto.v1.WriteGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

/**
 * Resume-from-min across shards orders checkpoints by <b>lineage</b> (Iceberg sequence numbers,
 * task-196), never by the raw snapshot id — ids are random longs, so the old {@code Math.min}
 * picked the wrong shard about half the time two shards diverged (task-205: a permanent
 * {@code CHECKPOINT_GAP} stall). Node-reported sequences win; the table's own {@link
 * SnapshotLineage} backfills legacy stored checkpoints; only a snapshot unknown everywhere
 * degrades to the numeric fallback.
 */
class ShardedWriteClientResumeTest {

  /** Ids deliberately numerically reversed vs. lineage: seq order is 700 → 900 → 50. */
  @Test
  void resumePicksTheLineageMinNotTheNumericMin() throws IOException, InterruptedException {
    List<Server> servers = new ArrayList<>();
    try {
      ShardedWriteClient client =
          clientOver(
              servers,
              at(900, 2), // mid
              at(50, 3), // ahead (numerically smallest — the old Math.min picked this, wrongly)
              at(700, 1)); // the actual laggard
      assertEquals(700L, client.checkpointSnapshotId(), "lineage min, not numeric min");
      client.close();
    } finally {
      servers.forEach(Server::shutdownNow);
    }
  }

  @Test
  void convergedShardsNeedNoOrderAndAnyEmptyShardResetsResume()
      throws IOException, InterruptedException {
    List<Server> servers = new ArrayList<>();
    try {
      // All shards at the same position: no sequence numbers needed at all.
      ShardedWriteClient converged = clientOver(servers, at(42, 0), at(42, 0), at(42, 0));
      assertEquals(42L, converged.checkpointSnapshotId());
      converged.close();

      // Any shard with no checkpoint ⇒ start from the beginning.
      List<Server> more = new ArrayList<>();
      try {
        ShardedWriteClient bootstrap = clientOver(more, at(42, 7), none());
        assertNull(bootstrap.checkpointSnapshotId());
        bootstrap.close();
      } finally {
        more.forEach(Server::shutdownNow);
      }
    } finally {
      servers.forEach(Server::shutdownNow);
    }
  }

  /** Legacy stored checkpoints (no sequence on the Node) are backfilled from table lineage. */
  @Test
  void tableLineageBackfillsLegacyNodeCheckpoints() throws IOException, InterruptedException {
    List<Server> servers = new ArrayList<>();
    try {
      SnapshotLineage lineage =
          id -> {
            Long seq = Map.of(900L, 2L, 50L, 3L).get(id);
            return seq == null ? OptionalLong.empty() : OptionalLong.of(seq);
          };
      ShardedWriteClient client =
          clientOver(lineage, servers, at(900, 0), at(50, 0)); // Node reports no sequences
      assertEquals(900L, client.checkpointSnapshotId(), "lineage-backfilled min (seq 2 < 3)");
      client.close();
    } finally {
      servers.forEach(Server::shutdownNow);
    }
  }

  /** Unknown everywhere (expired + never stamped): the pre-existing numeric fallback, warned. */
  @Test
  void unknownSequencesDegradeToTheNumericFallback() throws IOException, InterruptedException {
    List<Server> servers = new ArrayList<>();
    try {
      ShardedWriteClient client = clientOver(servers, at(900, 0), at(50, 0));
      assertEquals(50L, client.checkpointSnapshotId(), "documented legacy fallback");
      client.close();
    } finally {
      servers.forEach(Server::shutdownNow);
    }
  }

  // ---- fixtures ----

  private record Cp(long id, long seq, boolean present) {}

  private static Cp at(long id, long seq) {
    return new Cp(id, seq, true);
  }

  private static Cp none() {
    return new Cp(0, 0, false);
  }

  private static ShardedWriteClient clientOver(List<Server> servers, Cp... checkpoints)
      throws IOException {
    return clientOver(SnapshotLineage.none(), servers, checkpoints);
  }

  private static ShardedWriteClient clientOver(
      SnapshotLineage lineage, List<Server> servers, Cp... checkpoints) throws IOException {
    List<String> endpoints = new ArrayList<>();
    for (Cp cp : checkpoints) {
      Server s = ServerBuilder.forPort(0).addService(new CheckpointOnly(cp)).build().start();
      servers.add(s);
      endpoints.add("127.0.0.1:" + s.getPort());
    }
    return new ShardedWriteClient(
        endpoints, new ShardRouter(endpoints.size(), ShardRouter.Strategy.HASH), lineage);
  }

  private static final class CheckpointOnly extends WriteGrpc.WriteImplBase {
    private final Cp cp;

    CheckpointOnly(Cp cp) {
      this.cp = cp;
    }

    @Override
    public void getCheckpoint(GetCheckpointRequest request, StreamObserver<GetCheckpointResponse> obs) {
      GetCheckpointResponse.Builder response = GetCheckpointResponse.newBuilder();
      if (cp.present()) {
        SourceCheckpoint.Builder checkpoint =
            SourceCheckpoint.newBuilder().setIcebergSnapshot(cp.id());
        if (cp.seq() > 0) {
          checkpoint.setIcebergSequenceNumber(cp.seq());
        }
        response.setCheckpoint(checkpoint);
      }
      obs.onNext(response.build());
      obs.onCompleted();
    }
  }
}
