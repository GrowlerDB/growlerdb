package io.growlerdb.trino;

import io.airlift.slice.Slices;
import io.growlerdb.proto.v1.SearchHit;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionSplitProcessor;
import io.trino.spi.type.Type;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Executes one {@code growlerdb_search} invocation on a worker (task-51): re-runs the query against
 * the GrowlerDB endpoint and emits the matching keys + score as a single {@link Page} matching the
 * schema {@link GrowlerDbSearchFunction#analyze} returned. One split → one page → finished.
 */
public class GrowlerDbSearchProcessor implements TableFunctionSplitProcessor {

  private final GrowlerDbSearchHandle handle;
  private boolean produced;

  public GrowlerDbSearchProcessor(GrowlerDbSearchHandle handle) {
    this.handle = handle;
  }

  @Override
  public TableFunctionProcessorState process() {
    if (produced) {
      return TableFunctionProcessorState.Finished.FINISHED;
    }
    produced = true;

    List<SearchColumns.Kind> kinds =
        handle.getColumnKinds().stream()
            .map(SearchColumns.Kind::valueOf)
            .collect(Collectors.toList());
    List<Type> types = kinds.stream().map(SearchColumns::trinoType).collect(Collectors.toList());

    List<SearchHit> hits;
    try (SearchClient client = new SearchClient(handle.getHost(), handle.getPort())) {
      hits = client.search(handle.getQuery(), handle.getLimit());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("GrowlerDB search interrupted", e);
    }

    PageBuilder page = new PageBuilder(types);
    for (SearchHit hit : hits) {
      page.declarePosition();
      Object[] cells = SearchColumns.rowValues(hit, types.size());
      for (int c = 0; c < types.size(); c++) {
        writeCell(types.get(c), kinds.get(c), page.getBlockBuilder(c), cells[c]);
      }
    }
    return TableFunctionProcessorState.Processed.produced(page.build());
  }

  private static void writeCell(Type type, SearchColumns.Kind kind, BlockBuilder builder, Object v) {
    if (v == null) {
      builder.appendNull();
      return;
    }
    switch (kind) {
      case VARCHAR -> type.writeSlice(builder, Slices.utf8Slice((String) v));
      case BIGINT -> type.writeLong(builder, (Long) v);
      case DOUBLE -> type.writeDouble(builder, (Double) v);
      case BOOLEAN -> type.writeBoolean(builder, (Boolean) v);
      // TIMESTAMP_MICROS is a short timestamp: its native value is the epoch-micros long itself.
      case TIMESTAMP -> type.writeLong(builder, (Long) v);
    }
  }
}
