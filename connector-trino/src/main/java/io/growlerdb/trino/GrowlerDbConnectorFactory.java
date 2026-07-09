package io.growlerdb.trino;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import java.util.Map;

/** Registers the connector under {@code connector.name=growlerdb} (etc/catalog/growlerdb.properties). */
public class GrowlerDbConnectorFactory implements ConnectorFactory {

  @Override
  public String getName() {
    return "growlerdb";
  }

  @Override
  public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
    return new GrowlerDbConnector();
  }
}
