package io.growlerdb.trino;

import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import java.util.List;

/**
 * Trino plugin entry point (task-51): registers the GrowlerDB connector, whose only surface is the
 * {@code growlerdb_search} polymorphic table function — boolean retrieval over a GrowlerDB index that
 * returns matching keys + score to {@code JOIN} against the source Iceberg table (search-then-join,
 * D5 / wiki-07). Loaded by Trino from {@code etc/catalog/growlerdb.properties} (connector.name=growlerdb).
 */
public class GrowlerDbPlugin implements Plugin {

  @Override
  public Iterable<ConnectorFactory> getConnectorFactories() {
    return List.of(new GrowlerDbConnectorFactory());
  }
}
