package io.growlerdb.trino;

import io.airlift.slice.Slice;
import io.growlerdb.proto.v1.SearchHit;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ReturnTypeSpecification;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The {@code growlerdb_search} polymorphic table function:
 *
 * <pre>{@code
 * SELECT e.*
 * FROM lake.events e
 * JOIN TABLE(growlerdb.system.search(
 *        endpoint => 'gateway-host', port => 50061,
 *        query    => 'body:error AND env:prod', "limit" => 1000)) m
 *   ON e.id = m.id
 * ORDER BY m.growlerdb_score DESC;
 * }</pre>
 *
 * Boolean retrieval runs in GrowlerDB; the function returns the matching keys (one column per key
 * field) + a {@code growlerdb_score} double, which you JOIN against the source Iceberg table.
 * {@code analyze} learns the result schema from the index's key fields; execution re-runs the query.
 */
public class GrowlerDbSearchFunction extends AbstractConnectorTableFunction {

  private static final long DEFAULT_PORT = 50061L;
  private static final long DEFAULT_LIMIT = 1000L;

  public GrowlerDbSearchFunction() {
    super(
        "system",
        "search",
        List.of(
            ScalarArgumentSpecification.builder().name("ENDPOINT").type(VarcharType.VARCHAR).build(),
            ScalarArgumentSpecification.builder()
                .name("PORT")
                .type(IntegerType.INTEGER)
                .defaultValue(DEFAULT_PORT)
                .build(),
            ScalarArgumentSpecification.builder().name("QUERY").type(VarcharType.VARCHAR).build(),
            ScalarArgumentSpecification.builder()
                .name("LIMIT")
                .type(IntegerType.INTEGER)
                .defaultValue(DEFAULT_LIMIT)
                .build()),
        ReturnTypeSpecification.GenericTable.GENERIC_TABLE);
  }

  @Override
  public TableFunctionAnalysis analyze(
      ConnectorSession session,
      ConnectorTransactionHandle transaction,
      Map<String, Argument> arguments,
      ConnectorAccessControl accessControl) {
    String host = stringArg(arguments.get("ENDPOINT"));
    int port = (int) longArg(arguments.get("PORT"));
    String query = stringArg(arguments.get("QUERY"));
    int limit = (int) longArg(arguments.get("LIMIT"));

    // Learn the column schema from the index's key fields — one hit is enough; execution re-runs
    // the full query on the worker.
    List<SearchHit> sample;
    try (SearchClient client = new SearchClient(host, port)) {
      sample = client.search(query, 1);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("GrowlerDB search interrupted", e);
    }
    List<String> names = SearchColumns.columnNames(sample);
    List<SearchColumns.Kind> kinds = SearchColumns.columnKinds(sample);
    List<Type> types = kinds.stream().map(SearchColumns::trinoType).collect(Collectors.toList());

    GrowlerDbSearchHandle handle =
        new GrowlerDbSearchHandle(
            host, port, query, limit, names,
            kinds.stream().map(Enum::name).collect(Collectors.toList()));
    return TableFunctionAnalysis.builder()
        .returnedType(Descriptor.descriptor(names, types))
        .handle(handle)
        .build();
  }

  private static String stringArg(Argument argument) {
    return ((Slice) ((ScalarArgument) argument).getValue()).toStringUtf8();
  }

  private static long longArg(Argument argument) {
    return (Long) ((ScalarArgument) argument).getValue();
  }
}
