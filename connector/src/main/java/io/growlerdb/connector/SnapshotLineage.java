package io.growlerdb.connector;

import io.growlerdb.proto.v1.SourceCheckpoint;
import java.util.OptionalLong;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.SparkSession;

/**
 * Lineage order over Iceberg snapshots: snapshot ids are random longs and carry no
 * order, so anything that must compare two checkpoints — the Node's window-covering
 * continuity guard, the resume-from-min across shards, idempotency pruning — orders by the
 * snapshot's <b>data sequence number</b>, which is strictly monotone along a branch (format v2).
 * The connector stamps it on every checkpoint it sends; this is where it comes from.
 *
 * <p>Empty for an unknown/expired snapshot or a v1 table (Iceberg reports sequence 0) — every
 * consumer then falls back to the exact-match semantics, which are safe, just wedgeable.
 */
@FunctionalInterface
public interface SnapshotLineage {

  /** The snapshot's data sequence number, or empty when unknown (expired snapshot, v1 table). */
  OptionalLong sequenceOf(long snapshotId);

  /** Build the checkpoint proto for {@code snapshotId}, stamped with its sequence when known. */
  default SourceCheckpoint checkpoint(long snapshotId) {
    SourceCheckpoint.Builder cp = SourceCheckpoint.newBuilder().setIcebergSnapshot(snapshotId);
    sequenceOf(snapshotId).ifPresent(cp::setIcebergSequenceNumber);
    return cp.build();
  }

  /** No lineage info: every checkpoint is unordered. */
  static SnapshotLineage none() {
    return snapshotId -> OptionalLong.empty();
  }

  /**
   * Lineage from the table's live metadata via the Iceberg Java API ({@code
   * Spark3Util.loadIcebergTable} — the {@code .snapshots} metadata table does not expose the
   * sequence number). Refreshes once when a snapshot isn't found, since the head moves between
   * triggers. Degrades to {@link #none()} with a warning when the table can't be loaded —
   * sequence stamping is an upgrade, not a requirement.
   */
  static SnapshotLineage forTable(SparkSession spark, String qualifiedName) {
    final Table table;
    try {
      table = Spark3Util.loadIcebergTable(spark, qualifiedName);
    } catch (Exception e) {
      System.err.printf(
          "SnapshotLineage: cannot load %s via the Iceberg API (%s) — checkpoints will carry no "
              + "sequence numbers (exact-match continuity only)%n",
          qualifiedName, e);
      return none();
    }
    return snapshotId -> {
      Snapshot snap = table.snapshot(snapshotId);
      if (snap == null) {
        table.refresh();
        snap = table.snapshot(snapshotId);
      }
      return snap == null || snap.sequenceNumber() <= 0
          ? OptionalLong.empty()
          : OptionalLong.of(snap.sequenceNumber());
    };
  }
}
