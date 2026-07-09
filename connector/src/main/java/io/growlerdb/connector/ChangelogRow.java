package io.growlerdb.connector;

import io.growlerdb.proto.v1.Value;
import java.util.Map;

/**
 * One Iceberg changelog row, already decoded into wire {@link Value}s — the input
 * to {@link ChangelogMapper}. Reading these from Spark (the {@code _change_type} /
 * {@code _change_ordinal} / {@code _commit_snapshot_id} columns + {@code _metadata}
 * file/position) is task-63; this type keeps the mapping free of Spark.
 */
public final class ChangelogRow {

  public final ChangeType changeType;
  public final long changeOrdinal;
  public final long commitSnapshotId;
  /** Column name → value (key columns + indexed fields). */
  public final Map<String, Value> columns;
  /** Source data-file path the row came from (for the locator). */
  public final String icebergFile;
  /** Row position within {@link #icebergFile}. */
  public final long rowPosition;

  public ChangelogRow(
      ChangeType changeType,
      long changeOrdinal,
      long commitSnapshotId,
      Map<String, Value> columns,
      String icebergFile,
      long rowPosition) {
    this.changeType = changeType;
    this.changeOrdinal = changeOrdinal;
    this.commitSnapshotId = commitSnapshotId;
    this.columns = columns;
    this.icebergFile = icebergFile;
    this.rowPosition = rowPosition;
  }
}
