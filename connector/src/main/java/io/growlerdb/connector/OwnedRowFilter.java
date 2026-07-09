package io.growlerdb.connector;

import io.growlerdb.proto.v1.Coordinates;
import io.growlerdb.proto.v1.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

/**
 * Executor-side changelog filter for a shard-group worker (task-196): keep only the rows whose
 * key routes to one of the worker's owned shards. This is what makes the set a real scale-out —
 * without it every worker's driver pulls and maps the FULL changelog and only the write RPCs
 * shrink; with it each driver sees ~1/W of the rows, executor-side.
 *
 * <p>Routing is per key, and every op of a key routes to the same shard, so UPDATE_BEFORE /
 * UPDATE_AFTER pairs and per-key last-write-wins stay intact within one worker. A row missing a
 * key column is KEPT — the mapper throws loudly on it, which beats silently dropping it here.
 * The under-read gate counts the UNFILTERED changelog (a global-window assertion), so it runs
 * before this filter is applied.
 */
final class OwnedRowFilter implements FilterFunction<Row> {

  private final List<String> partitionFields;
  private final List<String> identifierFields;
  private final ShardRouter router;
  private final Set<Integer> owned;

  private OwnedRowFilter(IndexMapping mapping, ShardRouter router, Set<Integer> owned) {
    // Copy to guaranteed-serializable collections; the lambda-free class form keeps the Spark
    // closure cleaner to reason about.
    this.partitionFields = new ArrayList<>(mapping.partitionFields);
    this.identifierFields = new ArrayList<>(mapping.identifierFields);
    this.router = router;
    this.owned = new TreeSet<>(owned);
  }

  static OwnedRowFilter of(IndexMapping mapping, ShardRouter router, Set<Integer> owned) {
    return new OwnedRowFilter(mapping, router, owned);
  }

  @Override
  public boolean call(Row row) {
    Coordinates.Builder coords = Coordinates.newBuilder();
    for (String name : partitionFields) {
      Field field = field(row, name);
      if (field == null) {
        return true; // missing key column: keep — the mapper fails loudly on it
      }
      coords.addPartition(field);
    }
    for (String name : identifierFields) {
      Field field = field(row, name);
      if (field == null) {
        return true;
      }
      coords.addIdentifier(field);
    }
    return owned.contains(router.route(coords.build()));
  }

  private static Field field(Row row, String name) {
    Object value = row.getAs(name);
    if (value == null) {
      return null;
    }
    return Field.newBuilder().setName(name).setValue(ChangelogReader.toValue(value)).build();
  }
}
