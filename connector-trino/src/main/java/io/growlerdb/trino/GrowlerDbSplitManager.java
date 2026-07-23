package io.growlerdb.trino;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import java.util.Set;

/** One split per {@code growlerdb_search} call — the handle carries the query the worker runs. */
public class GrowlerDbSplitManager implements ConnectorSplitManager {

  @Override
  public ConnectorSplitSource getSplits(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorTableFunctionHandle function) {
    return new FixedSplitSource(new GrowlerDbSplit());
  }

  // The connector exposes no tables, only the growlerdb_search table function, so Trino never plans
  // a table scan against it. Required since Trino 483 made the table-scan overload abstract.
  @Override
  public ConnectorSplitSource getSplits(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorTableHandle table,
      Set<ColumnHandle> columns,
      Constraint constraint) {
    throw new UnsupportedOperationException("GrowlerDB exposes no tables, only growlerdb_search");
  }
}
