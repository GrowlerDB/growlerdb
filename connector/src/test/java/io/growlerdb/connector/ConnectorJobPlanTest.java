package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.growlerdb.connector.ConnectorJob.Commit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ConnectorJob#plan} — the bounded-catch-up windowing. Pure list
 * logic, no Spark: split an ordinal-sorted changelog window into sub-batches capped at N rows, cut
 * only at snapshot boundaries, each carrying a valid snapshot checkpoint.
 */
class ConnectorJobPlanTest {

  /** A changelog row in {@code snapshot} (only the snapshot id matters to windowing). */
  private static ChangelogRow row(long snapshot) {
    return new ChangelogRow(ChangeType.INSERT, snapshot, snapshot, Map.of(), "f", 0);
  }

  private static List<ChangelogRow> rows(long snapshot, int n) {
    List<ChangelogRow> out = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      out.add(row(snapshot));
    }
    return out;
  }

  @Test
  void underReadGateThrowsOnAShortfallAndIsExemptOtherwise() {
    // observed < expected → a loud stall (the checkpoint must not advance over the gap).
    IngestUnderReadException ex =
        assertThrows(
            IngestUnderReadException.class,
            () -> ConnectorJob.assertNotUnderRead("demo.ns.docs", 5L, 10L, 4, 6));
    assertTrue(ex.getMessage().contains("INGEST_UNDER_READ"), ex.getMessage());
    assertTrue(ex.getMessage().contains("2-row gap"), ex.getMessage()); // expected(6) - observed(4)

    // observed == expected (exact) and observed > expected (never an under-read) both pass.
    assertDoesNotThrow(() -> ConnectorJob.assertNotUnderRead("t", 5L, 10L, 6, 6));
    assertDoesNotThrow(() -> ConnectorJob.assertNotUnderRead("t", 5L, 10L, 7, 6));
    // expected < 0 marks the window exempt (overwrite/delete/unmaterialized) — skipped even at 0 rows.
    assertDoesNotThrow(() -> ConnectorJob.assertNotUnderRead("t", null, 10L, 0, -1));
  }

  @Test
  void emptyWindowStillCommitsToAdvanceTheCheckpoint() {
    List<Commit> commits = ConnectorJob.plan(List.of(), 50, 5L, 10L);
    assertEquals(1, commits.size());
    assertEquals(5L, commits.get(0).fromExclusive);
    assertEquals(10L, commits.get(0).checkpointSnapshot); // advances to head even with no rows
    assertTrue(commits.get(0).rows.isEmpty());
  }

  @Test
  void underTheCapIsASingleCommitToTheHead() {
    List<Commit> commits = ConnectorJob.plan(rows(10, 2), 50, 5L, 10L);
    assertEquals(1, commits.size());
    assertEquals(5L, commits.get(0).fromExclusive);
    assertEquals(10L, commits.get(0).checkpointSnapshot);
    assertEquals(2, commits.get(0).rows.size());
  }

  @Test
  void nullStartIsCarriedAsTheFirstCheckpointBoundary() {
    List<Commit> commits = ConnectorJob.plan(rows(10, 1), 50, null, 10L);
    assertEquals(1, commits.size());
    assertNull(commits.get(0).fromExclusive); // from the start of the changelog
    assertEquals(10L, commits.get(0).checkpointSnapshot);
  }

  @Test
  void splitsAtSnapshotBoundariesOnceOverTheCap() {
    // s10×3, s20×3, s30×2; cap 4. The cap is reached crossing s20→s30 (buffer = 6), so the cut
    // falls there; s30 lands in the final commit at the head.
    List<ChangelogRow> window = new ArrayList<>();
    window.addAll(rows(10, 3));
    window.addAll(rows(20, 3));
    window.addAll(rows(30, 2));

    List<Commit> commits = ConnectorJob.plan(window, 4, 5L, 30L);

    assertEquals(2, commits.size());
    // First commit: s10+s20, checkpoint at the last fully-buffered snapshot (20).
    assertEquals(5L, commits.get(0).fromExclusive);
    assertEquals(20L, commits.get(0).checkpointSnapshot);
    assertEquals(6, commits.get(0).rows.size());
    // Second commit resumes from 20 and advances to the head (30).
    assertEquals(20L, commits.get(1).fromExclusive);
    assertEquals(30L, commits.get(1).checkpointSnapshot);
    assertEquals(2, commits.get(1).rows.size());
    // No commit ever splits a snapshot (each row's snapshot ≤ its commit checkpoint).
    for (Commit c : commits) {
      for (ChangelogRow r : c.rows) {
        assertTrue(r.commitSnapshotId <= c.checkpointSnapshot);
      }
    }
  }

  @Test
  void aSingleSnapshotLargerThanTheCapCommitsWhole() {
    // One snapshot with 5 rows, cap 2 — never a boundary, so it commits as one batch (the Node's
    // raised gRPC limit covers it). The cut must never split a snapshot.
    List<Commit> commits = ConnectorJob.plan(rows(10, 5), 2, null, 10L);
    assertEquals(1, commits.size());
    assertEquals(10L, commits.get(0).checkpointSnapshot);
    assertEquals(5, commits.get(0).rows.size());
  }

  // --- streaming ---------------------------------------------------

  @Test
  void streamCommitsFlushesTheSameCutsAsPlanToTheSink() {
    // Mirrors splitsAtSnapshotBoundariesOnceOverTheCap, but streamed: the sink must receive each
    // commit as it closes, cut at the same snapshot boundaries.
    List<ChangelogRow> window = new ArrayList<>();
    window.addAll(rows(10, 3));
    window.addAll(rows(20, 3));
    window.addAll(rows(30, 2));

    List<Commit> streamed = new ArrayList<>();
    ConnectorJob.streamCommits(window.iterator(), 4, 5L, 30L, streamed::add);

    assertEquals(2, streamed.size());
    assertEquals(20L, streamed.get(0).checkpointSnapshot);
    assertEquals(6, streamed.get(0).rows.size());
    assertEquals(20L, streamed.get(1).fromExclusive);
    assertEquals(30L, streamed.get(1).checkpointSnapshot);
  }

  @Test
  void streamCommitsHoldsOnlyBoundedChunksNotTheWholeWindow() {
    // 1000 rows, one snapshot each, cap 10 → the buffer never grows to the window size (the property
    // that keeps driver memory O(chunk), not O(window)). Fed from a lazy iterator that never
    // materializes the window.
    Iterator<ChangelogRow> lazy =
        new Iterator<>() {
          long i = 0;

          @Override
          public boolean hasNext() {
            return i < 1000;
          }

          @Override
          public ChangelogRow next() {
            long s = ++i;
            return new ChangelogRow(ChangeType.INSERT, s, s, Map.of(), "f", 0);
          }
        };

    int[] commits = {0};
    int[] maxChunk = {0};
    ConnectorJob.streamCommits(
        lazy,
        10,
        0L,
        1000L,
        c -> {
          commits[0]++;
          maxChunk[0] = Math.max(maxChunk[0], c.rows.size());
        });

    assertTrue(commits[0] > 1, "streamed many commits, not one giant batch");
    assertTrue(maxChunk[0] <= 10, "no chunk exceeded the cap: " + maxChunk[0]);
  }

  // --- bounded metadata walk ----------------------------------

  /** A `.snapshots` metadata row: (snapshot_id, parent_id, operation, added-records string). */
  private static Row snap(long id, Long parent, String op, String added) {
    return RowFactory.create(id, parent, op, added);
  }

  @Test
  void sumWindowAddedRecordsSumsAppendsAlongLineage() {
    Map<Long, Row> byId = new HashMap<>();
    byId.put(30L, snap(30L, 20L, "append", "2"));
    byId.put(20L, snap(20L, 10L, "append", "3"));
    byId.put(10L, snap(10L, null, "append", "99")); // the start — excluded from the window sum
    assertEquals(5, ConnectorJob.sumWindowAddedRecords(byId, 10L, 30L));
  }

  @Test
  void sumWindowAddedRecordsSkipsReplaceAndBailsOnRowLevelSnapshots() {
    Map<Long, Row> withReplace = new HashMap<>();
    withReplace.put(20L, snap(20L, 10L, "replace", null)); // layout-only compaction → 0 rows
    assertEquals(0, ConnectorJob.sumWindowAddedRecords(withReplace, 10L, 20L));

    Map<Long, Row> withOverwrite = new HashMap<>();
    withOverwrite.put(20L, snap(20L, 10L, "overwrite", null)); // net diff ≠ added-records → skip gate
    assertEquals(-1, ConnectorJob.sumWindowAddedRecords(withOverwrite, 10L, 20L));

    Map<Long, Row> appendNoCount = new HashMap<>();
    appendNoCount.put(20L, snap(20L, 10L, "append", null)); // append without a count → can't assert
    assertEquals(-1, ConnectorJob.sumWindowAddedRecords(appendNoCount, 10L, 20L));
  }

  @Test
  void sumWindowAddedRecordsReportsIncompleteWhenAnAncestorIsMissing() {
    // The bounded (time-filtered) map is missing snapshot 20 → the walk can't reach the start, so it
    // returns the INCOMPLETE sentinel (Long.MIN_VALUE) and the caller retries with a full scan.
    Map<Long, Row> filtered = new HashMap<>();
    filtered.put(30L, snap(30L, 20L, "append", "2"));
    assertEquals(Long.MIN_VALUE, ConnectorJob.sumWindowAddedRecords(filtered, 10L, 30L));
  }
}
