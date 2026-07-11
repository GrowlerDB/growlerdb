package io.growlerdb.connector;

import io.growlerdb.proto.v1.Coordinates;
import io.growlerdb.proto.v1.DocBatch;
import io.growlerdb.proto.v1.DocOp;
import io.growlerdb.proto.v1.Document;
import io.growlerdb.proto.v1.Field;
import io.growlerdb.proto.v1.LocatedDoc;
import io.growlerdb.proto.v1.SourceCheckpoint;
import io.growlerdb.proto.v1.Value;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Maps Iceberg changelog rows to a {@link DocBatch} of {@link DocOp}s, keyed by
 * the composite key (<a href="../../../../wiki/06-ingestion.md">CDC from
 * Iceberg</a>):
 *
 * <ul>
 *   <li>{@code INSERT} / {@code UPDATE_AFTER} → upsert (key + fields + locator);
 *   <li>{@code DELETE} / {@code UPDATE_BEFORE} → delete by key;
 *   <li>rows from a {@code replace}/compaction snapshot → <b>skipped</b> (a layout
 *       change, not content) — locators self-heal via verify-and-fall-back,
 *       so they must not be read as a flood of delete+insert.
 * </ul>
 *
 * Rows are processed in commit order by {@code _change_ordinal} (Iceberg assigns it
 * per snapshot in commit sequence — unlike the snapshot id, which is a random long)
 * with <b>last-write-wins per key</b>, so an UPDATE_BEFORE+UPDATE_AFTER pair
 * collapses to a single upsert.
 */
public final class ChangelogMapper {

  private final IndexMapping mapping;
  private final Set<Long> replaceSnapshotIds;

  /**
   * @param mapping the index key/field shape
   * @param replaceSnapshotIds snapshot ids that are layout-only rewrites
   *     (compaction/{@code replace}); their rows are skipped
   */
  public ChangelogMapper(IndexMapping mapping, Set<Long> replaceSnapshotIds) {
    this.mapping = mapping;
    this.replaceSnapshotIds = replaceSnapshotIds;
  }

  /** Reduce a changelog window to the effective batch to commit (no {@code from} — unguarded). */
  public DocBatch toBatch(List<ChangelogRow> rows, SourceCheckpoint checkpoint, String batchId) {
    return toBatch(rows, null, checkpoint, batchId, null);
  }

  /**
   * Reduce a changelog window to the effective batch to commit, stamped with the {@code from}
   * checkpoint it resumes from (the window's prior checkpoint, exclusive; {@code null} = from the
   * start). The Node's continuity guard uses {@code from} to refuse a batch that doesn't pick up
   * exactly where the shard left off.
   */
  public DocBatch toBatch(
      List<ChangelogRow> rows,
      SourceCheckpoint fromCheckpoint,
      SourceCheckpoint checkpoint,
      String batchId) {
    return toBatch(rows, fromCheckpoint, checkpoint, batchId, null);
  }

  /**
   * Reduce a changelog window, additionally stamped with the connector's {@code safeCheckpoint}
   * resume floor (the min committed checkpoint across shards this trigger resumed from; {@code null}
   * = none yet). The Node prunes idempotency records at/below it — those batches can never be
   * re-sent. The floor is the same for every sub-batch of a trigger (unlike {@code from}, which is
   * this window's start), and always {@code <=} every shard's checkpoint, so pruning stays sound.
   */
  public DocBatch toBatch(
      List<ChangelogRow> rows,
      SourceCheckpoint fromCheckpoint,
      SourceCheckpoint checkpoint,
      String batchId,
      SourceCheckpoint safeCheckpoint) {
    // `_change_ordinal` is assigned in commit order (all changes from one snapshot
    // share an ordinal), so it — NOT the snapshot id, which is a random long — is the
    // chronological key. Within an ordinal, order deletes before upserts so an
    // UPDATE_BEFORE+UPDATE_AFTER pair collapses to the AFTER state under LWW.
    List<ChangelogRow> ordered = new ArrayList<>(rows);
    ordered.sort(
        Comparator.comparingLong((ChangelogRow r) -> r.changeOrdinal)
            .thenComparingInt(r -> r.changeType.isDelete() ? 0 : 1));

    // Last-write-wins per key; layout-only (replace) rows produce no doc op.
    LinkedHashMap<String, DocOp> effective = new LinkedHashMap<>();
    for (ChangelogRow row : ordered) {
      if (replaceSnapshotIds.contains(row.commitSnapshotId)) {
        continue;
      }
      effective.put(keyString(row), toOp(row));
    }

    DocBatch.Builder batch =
        DocBatch.newBuilder().setCheckpoint(checkpoint).setBatchId(batchId);
    if (fromCheckpoint != null) {
      batch.setFromCheckpoint(fromCheckpoint);
    }
    if (safeCheckpoint != null) {
      batch.setSafeCheckpoint(safeCheckpoint);
    }
    effective.values().forEach(batch::addOps);
    return batch.build();
  }

  /** Map one row to its DocOp (upsert or delete). */
  DocOp toOp(ChangelogRow row) {
    Coordinates key = coordinates(row);
    if (row.changeType.isDelete()) {
      return DocOp.newBuilder().setDelete(key).build();
    }
    Document.Builder doc = Document.newBuilder().setKey(key);
    for (String name : mapping.fields) {
      Value value = row.columns.get(name);
      if (value != null) {
        doc.putFields(name, value);
      }
    }
    LocatedDoc located =
        LocatedDoc.newBuilder()
            .setDoc(doc)
            .setIcebergFile(row.icebergFile)
            .setRowPosition(row.rowPosition)
            .build();
    return DocOp.newBuilder().setUpsert(located).build();
  }

  private Coordinates coordinates(ChangelogRow row) {
    Coordinates.Builder coords = Coordinates.newBuilder();
    for (String name : mapping.partitionFields) {
      coords.addPartition(field(name, row));
    }
    for (String name : mapping.identifierFields) {
      coords.addIdentifier(field(name, row));
    }
    return coords.build();
  }

  private Field field(String name, ChangelogRow row) {
    Value value = row.columns.get(name);
    if (value == null) {
      throw new IllegalArgumentException("changelog row missing key column: " + name);
    }
    return Field.newBuilder().setName(name).setValue(value).build();
  }

  /** A type-tagged dedup key over the row's partition + identifier values. */
  private String keyString(ChangelogRow row) {
    StringBuilder sb = new StringBuilder();
    for (String name : mapping.partitionFields) {
      sb.append('p').append(name).append('=').append(valueString(row.columns.get(name))).append(';');
    }
    for (String name : mapping.identifierFields) {
      sb.append('i').append(name).append('=').append(valueString(row.columns.get(name))).append(';');
    }
    return sb.toString();
  }

  private static String valueString(Value value) {
    if (value == null) {
      return "∅";
    }
    return switch (value.getKindCase()) {
      case STR -> "S:" + value.getStr();
      case INT -> "I:" + value.getInt();
      case FLOAT -> "F:" + value.getFloat();
      case BOOL -> "B:" + value.getBool();
      case TS_MICROS -> "T:" + value.getTsMicros();
      case KIND_NOT_SET -> "∅";
    };
  }
}
