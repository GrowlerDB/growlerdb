package io.growlerdb.connector;

import io.growlerdb.proto.v1.Value;
import io.growlerdb.proto.v1.VariantColumn;
import java.util.List;
import java.util.Map;

/**
 * One Iceberg changelog row, already decoded into wire {@link Value}s — the input
 * to {@link ChangelogMapper}. Read from Spark (the {@code _change_type} /
 * {@code _change_ordinal} / {@code _commit_snapshot_id} columns); this type keeps the
 * mapping free of Spark. Store-less hydration re-finds the source row by key at read
 * time, so no source (file, position) is carried.
 */
public final class ChangelogRow {

  public final ChangeType changeType;
  public final long changeOrdinal;
  public final long commitSnapshotId;
  /** Column name → value (key columns + indexed fields). */
  public final Map<String, Value> columns;
  /** Extracted variant flatten columns (D47/D48) — one per variant column; empty when none. */
  public final List<VariantColumn> variants;

  public ChangelogRow(
      ChangeType changeType,
      long changeOrdinal,
      long commitSnapshotId,
      Map<String, Value> columns) {
    this(changeType, changeOrdinal, commitSnapshotId, columns, List.of());
  }

  public ChangelogRow(
      ChangeType changeType,
      long changeOrdinal,
      long commitSnapshotId,
      Map<String, Value> columns,
      List<VariantColumn> variants) {
    this.changeType = changeType;
    this.changeOrdinal = changeOrdinal;
    this.commitSnapshotId = commitSnapshotId;
    this.columns = columns;
    this.variants = variants;
  }
}
