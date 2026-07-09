package io.growlerdb.trino;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.transaction.IsolationLevel;
import java.util.Optional;
import java.util.Set;

/**
 * The GrowlerDB Trino connector (task-51): no tables, just the {@code growlerdb_search} table
 * function. Stateless — each call re-runs the search — so metadata is empty and one transaction
 * handle suffices.
 */
public class GrowlerDbConnector implements Connector {

  private final ConnectorMetadata metadata = new ConnectorMetadata() {};
  private final ConnectorTableFunction searchFunction = new GrowlerDbSearchFunction();
  private final GrowlerDbSplitManager splitManager = new GrowlerDbSplitManager();
  private final GrowlerDbFunctionProvider functionProvider = new GrowlerDbFunctionProvider();

  @Override
  public ConnectorTransactionHandle beginTransaction(
      IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
    return GrowlerDbTransactionHandle.INSTANCE;
  }

  @Override
  public ConnectorMetadata getMetadata(
      ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
    return metadata;
  }

  @Override
  public Set<ConnectorTableFunction> getTableFunctions() {
    return Set.of(searchFunction);
  }

  @Override
  public ConnectorSplitManager getSplitManager() {
    return splitManager;
  }

  @Override
  public Optional<FunctionProvider> getFunctionProvider() {
    return Optional.of(functionProvider);
  }
}
