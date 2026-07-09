package io.growlerdb.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import java.util.List;

/**
 * The {@code growlerdb_search} invocation, carried from planning (analyze) to the workers: the read
 * endpoint, the query, and the result schema (column names + kinds) learned at analyze time. JSON-
 * serializable (Trino ships handles across nodes); the kinds are the fixed {@link SearchColumns.Kind}
 * set as strings, so no TypeManager is needed to rebuild the columns on the worker.
 */
public class GrowlerDbSearchHandle implements ConnectorTableFunctionHandle {

  private final String host;
  private final int port;
  private final String query;
  private final int limit;
  private final List<String> columnNames;
  private final List<String> columnKinds;

  @JsonCreator
  public GrowlerDbSearchHandle(
      @JsonProperty("host") String host,
      @JsonProperty("port") int port,
      @JsonProperty("query") String query,
      @JsonProperty("limit") int limit,
      @JsonProperty("columnNames") List<String> columnNames,
      @JsonProperty("columnKinds") List<String> columnKinds) {
    this.host = host;
    this.port = port;
    this.query = query;
    this.limit = limit;
    this.columnNames = columnNames;
    this.columnKinds = columnKinds;
  }

  @JsonProperty public String getHost() { return host; }
  @JsonProperty public int getPort() { return port; }
  @JsonProperty public String getQuery() { return query; }
  @JsonProperty public int getLimit() { return limit; }
  @JsonProperty public List<String> getColumnNames() { return columnNames; }
  @JsonProperty public List<String> getColumnKinds() { return columnKinds; }
}
