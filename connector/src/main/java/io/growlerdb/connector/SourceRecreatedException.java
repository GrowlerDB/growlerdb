package io.growlerdb.connector;

/**
 * Thrown when the connector's resume checkpoint is no longer in the source table's lineage — the
 * source was dropped+recreated (or rolled back), so its current snapshot is not a descendant of the
 * one the index was last committed at. The index is stale and must be reindexed.
 *
 * <p>Composes with the Node's serve-time lineage guard, which refuses to serve the stale index until
 * a reindex re-anchors it.
 */
public final class SourceRecreatedException extends RuntimeException {

  public SourceRecreatedException(String table, long checkpoint, long head) {
    super(
        "SOURCE_RECREATED: source `"
            + table
            + "` was recreated or rolled back — its current snapshot "
            + head
            + " is not a descendant of the index's checkpoint "
            + checkpoint
            + ". The index is stale and must be reindexed; the connector cannot resume from a broken"
            + " lineage.");
  }
}
