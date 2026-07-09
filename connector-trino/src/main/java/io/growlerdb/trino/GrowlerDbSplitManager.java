package io.growlerdb.trino;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;

/** One split per {@code growlerdb_search} call — the handle carries the query the worker runs. */
public class GrowlerDbSplitManager implements ConnectorSplitManager {

  @Override
  public ConnectorSplitSource getSplits(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorTableFunctionHandle function) {
    return new FixedSplitSource(new GrowlerDbSplit());
  }
}
