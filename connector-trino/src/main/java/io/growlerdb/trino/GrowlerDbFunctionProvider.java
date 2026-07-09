package io.growlerdb.trino;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionSplitProcessor;

/** Supplies the worker-side executor for {@code growlerdb_search} (task-51). */
public class GrowlerDbFunctionProvider implements FunctionProvider {

  @Override
  public TableFunctionProcessorProvider getTableFunctionProcessorProvider(
      ConnectorTableFunctionHandle functionHandle) {
    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionSplitProcessor getSplitProcessor(
          ConnectorSession session, ConnectorTableFunctionHandle handle, ConnectorSplit split) {
        return new GrowlerDbSearchProcessor((GrowlerDbSearchHandle) handle);
      }
    };
  }
}
