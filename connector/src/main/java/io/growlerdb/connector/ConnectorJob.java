package io.growlerdb.connector;

import io.growlerdb.proto.v1.DocBatch;
import io.growlerdb.proto.v1.SourceCheckpoint;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * The connector pipeline for a single source→Node hop: read the Iceberg
 * changelog since a checkpoint ({@link ChangelogReader}) → reduce to a
 * {@link DocBatch} ({@link ChangelogMapper}) → commit it over the Write
 * gRPC ({@link WriteClient} → {@code growlerdb serve}).
 *
 * <p>{@link #runOnce} is one micro-batch: it reads the window from {@code
 * startSnapshotId} (the last committed checkpoint) to the table's current snapshot,
 * commits it, and returns the new checkpoint to resume from. The streaming wrapper
 * ({@link ConnectorApp#runStream}) drives this per trigger.
 *
 * <p><b>Exactly-once:</b> the batch carries its {@link SourceCheckpoint} and a
 * deterministic {@code batch_id} (the snapshot range), so the Node commits the write
 * and the checkpoint atomically and a replay is a no-op. The resume checkpoint is
 * passed in, obtained from the Node so it survives connector restarts.
 */
public final class ConnectorJob {

  /**
   * Default per-commit changelog-row cap: a catch-up window commits in sub-batches of at most this
   * many rows (cut at snapshot boundaries), so a large backlog never lands as one oversized
   * {@code Write} that blows the gRPC message limit / spikes memory. Well under the Node's decode cap.
   */
  public static final long DEFAULT_MAX_COMMIT_ROWS = 50_000;

  private final String catalog;
  private final String table;
  private final IndexMapping mapping;
  private final List<String> identifierColumns;
  private final Set<Long> replaceSnapshotIds;
  private final long maxCommitRows;
  /** Shard-group mode: executor-side filter to this worker's owned rows; null = all. */
  private final OwnedRowFilter ownedFilter;
  /** Variant extraction (D47/D48): walks each row's variant column into flatten leaves + shaped
   *  values. Null when the index maps no variant column. */
  private final VariantExtractor variantExtractor;

  /**
   * @param catalog Spark catalog name (e.g. {@code demo})
   * @param table table identifier within the catalog (e.g. {@code ns.docs})
   * @param mapping the index key/field shape rows are projected against
   * @param identifierColumns columns that pair UPDATE_BEFORE/UPDATE_AFTER in the
   *     changelog (typically the index identifier fields)
   */
  public ConnectorJob(
      String catalog, String table, IndexMapping mapping, List<String> identifierColumns) {
    this(catalog, table, mapping, identifierColumns, Set.of(), DEFAULT_MAX_COMMIT_ROWS);
  }

  /** As above, plus {@code replaceSnapshotIds} to skip (layout-only compactions). */
  public ConnectorJob(
      String catalog,
      String table,
      IndexMapping mapping,
      List<String> identifierColumns,
      Set<Long> replaceSnapshotIds) {
    this(catalog, table, mapping, identifierColumns, replaceSnapshotIds, DEFAULT_MAX_COMMIT_ROWS);
  }

  /** As above, plus the per-commit changelog-row cap for bounded catch-up. */
  public ConnectorJob(
      String catalog,
      String table,
      IndexMapping mapping,
      List<String> identifierColumns,
      Set<Long> replaceSnapshotIds,
      long maxCommitRows) {
    this(catalog, table, mapping, identifierColumns, replaceSnapshotIds, maxCommitRows, null);
  }

  private ConnectorJob(
      String catalog,
      String table,
      IndexMapping mapping,
      List<String> identifierColumns,
      Set<Long> replaceSnapshotIds,
      long maxCommitRows,
      OwnedRowFilter ownedFilter) {
    this(
        catalog,
        table,
        mapping,
        identifierColumns,
        replaceSnapshotIds,
        maxCommitRows,
        ownedFilter,
        null);
  }

  private ConnectorJob(
      String catalog,
      String table,
      IndexMapping mapping,
      List<String> identifierColumns,
      Set<Long> replaceSnapshotIds,
      long maxCommitRows,
      OwnedRowFilter ownedFilter,
      VariantExtractor variantExtractor) {
    this.catalog = catalog;
    this.table = table;
    this.mapping = mapping;
    this.identifierColumns = List.copyOf(identifierColumns);
    this.replaceSnapshotIds = Set.copyOf(replaceSnapshotIds);
    this.maxCommitRows = maxCommitRows > 0 ? maxCommitRows : DEFAULT_MAX_COMMIT_ROWS;
    this.ownedFilter = ownedFilter;
    this.variantExtractor = variantExtractor;
  }

  /** A copy of this job that extracts a variant column (D47/D48) via {@code extractor} —
   *  flatten leaves + discriminator-selected shape values per row. Composes with {@link #ownedBy}. */
  public ConnectorJob withVariant(VariantExtractor extractor) {
    return new ConnectorJob(
        catalog,
        table,
        mapping,
        identifierColumns,
        replaceSnapshotIds,
        maxCommitRows,
        ownedFilter,
        extractor);
  }

  /**
   * A copy of this job scoped to one worker of a parallel connector set: the changelog is filtered
   * executor-side to rows whose keys route to {@code owned} shards, so each worker's driver maps
   * ~1/W of the window. The under-read gate still counts the UNFILTERED (global) window.
   */
  public ConnectorJob ownedBy(ShardRouter router, Set<Integer> owned) {
    return new ConnectorJob(
        catalog,
        table,
        mapping,
        identifierColumns,
        replaceSnapshotIds,
        maxCommitRows,
        OwnedRowFilter.of(mapping, router, owned),
        variantExtractor);
  }

  /**
   * Read the changelog from {@code startSnapshotId} (exclusive; {@code null} = from
   * the start) to the table's current snapshot, commit it through {@code writeClient},
   * and return the outcome. A no-op (and no RPC) when the table is unborn or already
   * caught up.
   */
  public Result runOnce(SparkSession spark, Long startSnapshotId, BatchWriter writeClient) {
    Long current = currentSnapshotId(spark);
    if (current == null || Objects.equals(current, startSnapshotId)) {
      // Unborn table, or no new snapshot since the checkpoint — nothing to commit.
      return new Result(startSnapshotId, -1L, 0, false);
    }

    // Lineage guard: a resume checkpoint that isn't an ancestor of head means the source was
    // dropped+recreated (or rolled back) — fail fast with an actionable error instead of Iceberg's
    // cryptic ancestry failure. The index is stale and must be reindexed.
    if (startSnapshotId != null && !isAncestorOfHead(spark, startSnapshotId)) {
      throw new SourceRecreatedException(qualifiedName(), startSnapshotId, current);
    }

    Dataset<Row> changelog =
        ChangelogReader.changelog(
            spark,
            catalog,
            table,
            startSnapshotId,
            identifierColumns,
            variantExtractor == null ? null : variantExtractor.spec().column);

    // Expected-row-count gate against an under-read: assert the changelog carried at least as many
    // rows as the window's append snapshots committed (`added-records`) BEFORE any write. A shortfall
    // throws (no cursor advance) so the trigger re-reads on restart — a transient scan race self-heals
    // while a real gap stays a loud stall instead of a silent checkpoint over the loss.
    // `observed` is a DISTRIBUTED count so a large post-outage backlog can't OOM the driver.
    long observed = changelog.count();
    long expected = expectedAppendedRecords(spark, startSnapshotId, current);
    try {
      assertNotUnderRead(qualifiedName(), startSnapshotId, current, observed, expected);
    } catch (IngestUnderReadException e) {
      ConnectorMetrics.recordUnderRead(); // a metric that survives the rotating log
      throw e;
    }

    // Bounded catch-up: STREAM the window read→map→commit rather than materializing it in the
    // driver. `rowIterator` pulls one partition at a time and `streamCommits` flushes sub-batches
    // capped at `maxCommitRows` (cut only at snapshot boundaries), so driver memory is O(chunk).
    // Each sub-batch checkpoints at its end snapshot: the Node commits write+checkpoint atomically
    // and a restart mid-catch-up resumes exactly-once (`batch_id` dedups the boundary). The final
    // commit advances to head even if the tail window had no rows.
    // `safeCheckpoint` is the resume FLOOR (the min committed checkpoint across shards): stamped
    // identically on every sub-batch so the Node can prune those shards' idempotency records; `null`
    // (empty shard set) prunes nothing.
    // Shard-group mode drops rows other workers own AFTER the gate count above — ownership is per
    // key, so per-key op pairs and LWW stay intact within this worker's subset.
    if (ownedFilter != null) {
      changelog = changelog.filter(ownedFilter);
    }

    // Stamp each checkpoint with its lineage sequence number — the order the Node's window-covering
    // guard, resume-min, and idempotency pruning rely on (snapshot ids are random longs). Loaded via
    // the Iceberg Java API; degrades to unstamped exact-match semantics if unavailable or format v1.
    SnapshotLineage lineage = SnapshotLineage.forTable(spark, catalog + "." + table);
    SourceCheckpoint safeCheckpoint =
        startSnapshotId == null ? null : lineage.checkpoint(startSnapshotId);
    ChangelogMapper mapper = new ChangelogMapper(mapping, replaceSnapshotIds);
    long[] lastCommitted = {-1L};
    int[] totalOps = {0};
    streamCommits(
        ChangelogReader.rowIterator(changelog, projectedColumns(), variantExtractor),
        maxCommitRows,
        startSnapshotId,
        current,
        c -> {
          DocBatch batch =
              mapper.toBatch(
                  c.rows,
                  c.fromExclusive == null ? null : lineage.checkpoint(c.fromExclusive),
                  lineage.checkpoint(c.checkpointSnapshot),
                  batchId(c.fromExclusive, c.checkpointSnapshot),
                  safeCheckpoint);
          lastCommitted[0] = writeClient.write(batch);
          totalOps[0] += batch.getOpsCount();
        });
    // Per-trigger observability so a stalled/short trigger stays visible after the log rotates.
    // `expected < 0` means the gate didn't apply (overwrite/delete window); recordTrigger skips the
    // expected counter then.
    ConnectorMetrics.recordTrigger(observed, expected, current);
    return new Result(current, lastCommitted[0], totalOps[0], true);
  }

  /**
   * Split an ordinal-sorted changelog window into bounded commits. A commit is cut once the buffer
   * reaches {@code maxCommitRows} <i>and</i> the next row begins a different snapshot — so a commit
   * never splits a snapshot (UPDATE_BEFORE/UPDATE_AFTER pairs and per-snapshot LWW stay intact) and
   * an over-cap snapshot commits whole. Each commit checkpoints at its last row's snapshot; the final
   * commit checkpoints at {@code current} (head) so trailing data-less snapshots still advance.
   * Always returns at least one commit (possibly empty). Pure (no Spark/RPC) so it's unit-testable.
   */
  static List<Commit> plan(
      List<ChangelogRow> rows, long maxCommitRows, Long startSnapshotId, long current) {
    List<Commit> commits = new ArrayList<>();
    streamCommits(rows.iterator(), maxCommitRows, startSnapshotId, current, commits::add);
    return commits;
  }

  /**
   * Streaming form of {@link #plan}: cut the same bounded, snapshot-aligned commits, but feed each to
   * {@code sink} as it closes instead of buffering the whole plan, so a caller streaming rows from
   * {@link ChangelogReader#rowIterator} keeps driver memory O(chunk). Always emits at least one
   * commit (possibly empty) advancing to {@code current}. Pure (no Spark/RPC) so it's unit-testable.
   */
  static void streamCommits(
      Iterator<ChangelogRow> rows,
      long maxCommitRows,
      Long startSnapshotId,
      long current,
      Consumer<Commit> sink) {
    Long cursor = startSnapshotId;
    List<ChangelogRow> buf = new ArrayList<>();
    long bufEndSnapshot = -1L;
    while (rows.hasNext()) {
      ChangelogRow row = rows.next();
      if (!buf.isEmpty() && row.commitSnapshotId != bufEndSnapshot && buf.size() >= maxCommitRows) {
        sink.accept(new Commit(cursor, bufEndSnapshot, buf));
        cursor = bufEndSnapshot;
        buf = new ArrayList<>();
      }
      buf.add(row);
      bufEndSnapshot = row.commitSnapshotId;
    }
    sink.accept(new Commit(cursor, current, buf));
  }

  /** One bounded commit: the rows to write, the prior checkpoint, and the checkpoint to advance to. */
  static final class Commit {
    /** Checkpoint this commit resumes from (exclusive); {@code null} = the start of the changelog. */
    final Long fromExclusive;
    /** Snapshot id the index reflects after this commit — the new checkpoint. */
    final long checkpointSnapshot;
    /** Changelog rows in this commit (a contiguous, snapshot-aligned slice of the window). */
    final List<ChangelogRow> rows;

    Commit(Long fromExclusive, long checkpointSnapshot, List<ChangelogRow> rows) {
      this.fromExclusive = fromExclusive;
      this.checkpointSnapshot = checkpointSnapshot;
      this.rows = rows;
    }
  }

  /** Fully-qualified {@code catalog.table}. */
  public String qualifiedName() {
    return catalog + "." + table;
  }

  /**
   * Whether {@code snapshotId} is an ancestor of head — i.e. a valid resume point — via Iceberg's
   * {@code .history} {@code is_current_ancestor} flag. Not one (or absent, as after a drop+recreate)
   * means the lineage broke and the changelog read would fail the ancestry assertion.
   */
  boolean isAncestorOfHead(SparkSession spark, long snapshotId) {
    return !spark
        .sql(
            "SELECT 1 FROM "
                + catalog
                + "."
                + table
                + ".history WHERE snapshot_id = "
                + snapshotId
                + " AND is_current_ancestor LIMIT 1")
        .collectAsList()
        .isEmpty();
  }

  /**
   * The table's current snapshot id, or {@code null} if it has none yet. Resolved from the
   * <b>{@code main} branch ref</b> (lineage head), not {@code ORDER BY committed_at}: under a
   * two-writer clock skew the newest-by-time snapshot need not be the lineage tip, so ordering by
   * time could return a non-current head (a head-shadowing no-op stall). The {@code main} ref is the
   * authoritative branch tip the changelog scan resolves lineage to, keeping head and scan aligned.
   */
  Long currentSnapshotId(SparkSession spark) {
    List<Row> r =
        spark
            .sql(
                "SELECT snapshot_id FROM "
                    + catalog
                    + "."
                    + table
                    + ".refs WHERE name = 'main' AND type = 'BRANCH' LIMIT 1")
            .collectAsList();
    return r.isEmpty() ? null : r.get(0).getLong(0);
  }

  /**
   * Records the source physically committed across the window {@code (startSnapshotId, current]} —
   * {@code Σ summary['added-records']} over its {@code append} snapshots — or {@code -1} when the
   * count isn't soundly comparable to the changelog rows (so the {@linkplain IngestUnderReadException
   * gate} is skipped). The window is walked along <b>lineage</b> (parent_id back from {@code
   * current}), not by time, matching {@link #currentSnapshotId}. The gate is exact only for
   * append/layout-only-{@code replace} windows; an {@code overwrite}/{@code delete} snapshot makes the
   * changelog's net diff diverge from physical {@code added-records}, so we return {@code -1} and let
   * reconcile be the backstop.
   */
  /** Walk hit an ancestor absent from the (possibly time-filtered) snapshot map — retry unbounded. */
  private static final long INCOMPLETE_LINEAGE = Long.MIN_VALUE;

  long expectedAppendedRecords(SparkSession spark, Long startSnapshotId, long current) {
    // Bounded metadata scan: filter the `.snapshots` collect to `committed_at >=
    // committed_at(startSnapshotId)` rather than collecting all history every trigger. Under
    // multi-writer clock skew a descendant can record an EARLIER `committed_at` and fall outside the
    // filter; if the lineage walk then can't reach `startSnapshotId`, fall back to the full scan so
    // the gate stays exact. A null `startSnapshotId` (bootstrap) has no lower bound.
    Long fromMillis = startSnapshotId == null ? null : committedAtMillis(spark, startSnapshotId);
    long total =
        sumWindowAddedRecords(loadSnapshotLineage(spark, fromMillis), startSnapshotId, current);
    if (total == INCOMPLETE_LINEAGE && fromMillis != null) {
      total = sumWindowAddedRecords(loadSnapshotLineage(spark, null), startSnapshotId, current);
    }
    // INCOMPLETE after the unbounded retry ⇒ lineage genuinely not materialized (e.g. expired) —
    // treat as the gate-skip sentinel, same as an overwrite/delete window.
    return total == INCOMPLETE_LINEAGE ? -1 : total;
  }

  /** Epoch-millis {@code committed_at} of a snapshot, or {@code null} if it isn't in `.snapshots`. */
  private long committedAtMillis(SparkSession spark, long snapshotId) {
    List<Row> r =
        spark
            .sql(
                "SELECT unix_millis(committed_at) FROM "
                    + catalog
                    + "."
                    + table
                    + ".snapshots WHERE snapshot_id = "
                    + snapshotId)
            .collectAsList();
    return (r.isEmpty() || r.get(0).isNullAt(0)) ? null : r.get(0).getLong(0);
  }

  /**
   * {@code snapshot_id -> (parent_id, operation, added-records)} for the lineage-walk, optionally
   * bounded to snapshots with {@code committed_at >= fromMillis} (the window); {@code null}
   * collects the full history.
   */
  private java.util.Map<Long, Row> loadSnapshotLineage(SparkSession spark, Long fromMillis) {
    String where = fromMillis == null ? "" : " WHERE unix_millis(committed_at) >= " + fromMillis;
    List<Row> snaps =
        spark
            .sql(
                "SELECT snapshot_id, parent_id, operation, summary['added-records'] AS added FROM "
                    + catalog
                    + "."
                    + table
                    + ".snapshots"
                    + where)
            .collectAsList();
    java.util.Map<Long, Row> byId = new java.util.HashMap<>();
    for (Row s : snaps) {
      byId.put(s.getLong(0), s);
    }
    return byId;
  }

  /**
   * Σ {@code added-records} over the append snapshots on the lineage from {@code current} back to
   * (exclusive) {@code startSnapshotId}. Returns {@code -1} when the count isn't soundly comparable
   * (an overwrite/delete snapshot, or an append with no count), or {@link #INCOMPLETE_LINEAGE} when
   * an ancestor is absent from {@code byId} (so the caller can retry with a wider scan).
   */
  static long sumWindowAddedRecords(
      java.util.Map<Long, Row> byId, Long startSnapshotId, long current) {
    long total = 0;
    Long id = current;
    while (id != null && !Objects.equals(id, startSnapshotId)) {
      Row s = byId.get(id);
      if (s == null) {
        return INCOMPLETE_LINEAGE; // ancestor missing from this (possibly filtered) map
      }
      String operation = s.isNullAt(2) ? "" : s.getString(2);
      switch (operation) {
        case "append" -> {
          if (s.isNullAt(3)) {
            return -1; // an append with no added-records count — can't assert exactly
          }
          total += Long.parseLong(s.getString(3));
        }
        case "replace" -> {
          // Layout-only compaction — transparent to the changelog scan; contributes no rows.
        }
        default -> {
          // overwrite/delete/… : row-level deletes/updates make net diff ≠ physical added-records.
          return -1;
        }
      }
      id = s.isNullAt(1) ? null : s.getLong(1);
    }
    // Walking off the root without meeting startSnapshotId means the checkpoint isn't an ancestor —
    // isAncestorOfHead already handles that before we get here.
    return total;
  }

  /** Key + field columns to carry off the changelog, de-duplicated, stable order. */
  List<String> projectedColumns() {
    LinkedHashSet<String> cols = new LinkedHashSet<>();
    cols.addAll(mapping.partitionFields);
    cols.addAll(mapping.identifierFields);
    cols.addAll(mapping.fields);
    // A variant column is read (and extracted) even though it isn't a scalar mapping field; a
    // sibling discriminator column must be read too so shape selection can see it.
    if (variantExtractor != null) {
      cols.add(variantExtractor.spec().column);
      String disc = variantExtractor.spec().discriminator;
      if (disc != null && !disc.startsWith(variantExtractor.spec().column + ".")) {
        cols.add(disc);
      }
    }
    return new ArrayList<>(cols);
  }

  /**
   * The expected-row-count gate decision: throw {@link IngestUnderReadException} when {@code
   * observed} rows fall short of {@code expected} (the window's committed append records). A negative
   * {@code expected} means the count isn't soundly comparable, so the gate is skipped. Pure (no
   * Spark) so the decision + message are unit-testable.
   */
  static void assertNotUnderRead(
      String table, Long fromExclusive, long head, long observed, long expected) {
    if (expected >= 0 && observed < expected) {
      throw new IngestUnderReadException(
          table, fromExclusive == null ? 0L : fromExclusive, head, observed, expected);
    }
  }

  /** Deterministic id for the snapshot window — the Node's idempotent-replay guard. */
  private String batchId(Long startSnapshotId, long current) {
    return table + "@" + (startSnapshotId == null ? "0" : startSnapshotId) + "->" + current;
  }

  /** Outcome of one {@link #runOnce}: the checkpoint to resume from + what committed. */
  public static final class Result {
    /** Snapshot the index now reflects — pass back as {@code startSnapshotId} next run. */
    public final Long checkpointSnapshotId;
    /** The Node's committed index snapshot, or {@code -1} when nothing was written. */
    public final long committedSnapshot;
    /** Number of doc ops in the committed batch. */
    public final int opCount;
    /** Whether a batch was actually committed (false = unborn/caught-up). */
    public final boolean wrote;

    Result(Long checkpointSnapshotId, long committedSnapshot, int opCount, boolean wrote) {
      this.checkpointSnapshotId = checkpointSnapshotId;
      this.committedSnapshot = committedSnapshot;
      this.opCount = opCount;
      this.wrote = wrote;
    }
  }
}
