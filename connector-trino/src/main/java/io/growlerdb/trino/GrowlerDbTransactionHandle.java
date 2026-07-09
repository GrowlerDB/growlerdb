package io.growlerdb.trino;

import io.trino.spi.connector.ConnectorTransactionHandle;

/** The connector is stateless (each query re-runs the search), so one transaction handle suffices. */
public enum GrowlerDbTransactionHandle implements ConnectorTransactionHandle {
  INSTANCE
}
