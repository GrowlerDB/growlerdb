package io.growlerdb.trino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airlift.slice.Slices;
import io.growlerdb.proto.v1.Coordinates;
import io.growlerdb.proto.v1.Field;
import io.growlerdb.proto.v1.SearchGrpc;
import io.growlerdb.proto.v1.SearchHit;
import io.growlerdb.proto.v1.SearchRequest;
import io.growlerdb.proto.v1.SearchResponse;
import io.growlerdb.proto.v1.Value;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.VarcharType;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Drives the connector's real execution path (task-51) end to end against a loopback {@code Search}
 * server, using Trino's actual SPI types but no engine: {@link GrowlerDbSearchFunction#analyze}
 * (argument parsing + schema discovery → descriptor + handle), then {@link GrowlerDbSearchProcessor}
 * (re-running the query + building a {@link Page}), reading the values back out. Verifies the parts
 * that are ours — Page/Block building, the handle round-trip, the gRPC calls; Trino's engine-level
 * SQL binding of the PTF is the one layer left to a live Trino run.
 */
class GrowlerDbSearchExecutionTest {

  private static SearchHit hit(long day, String id, double score) {
    return SearchHit.newBuilder()
        .setCoordinates(
            Coordinates.newBuilder()
                .addPartition(
                    Field.newBuilder().setName("day").setValue(Value.newBuilder().setInt(day)))
                .addIdentifier(
                    Field.newBuilder().setName("id").setValue(Value.newBuilder().setStr(id))))
        .setScore(score)
        .build();
  }

  @Test
  void searchFunctionAnalyzesThenProducesAPageOfKeysAndScores() throws Exception {
    Server server =
        ServerBuilder.forPort(0)
            .addService(
                new SearchGrpc.SearchImplBase() {
                  @Override
                  public void search(SearchRequest req, StreamObserver<SearchResponse> obs) {
                    obs.onNext(
                        SearchResponse.newBuilder()
                            .addHits(hit(20, "d1", 1.5))
                            .addHits(hit(20, "d2", 0.5))
                            .build());
                    obs.onCompleted();
                  }
                })
            .build()
            .start();
    int port = server.getPort();
    try {
      // --- analyze: parse args, discover the schema from a sample hit, return descriptor + handle.
      Map<String, Argument> args =
          Map.of(
              "ENDPOINT",
                  new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("127.0.0.1")),
              "PORT", new ScalarArgument(IntegerType.INTEGER, (long) port),
              "QUERY", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("body:x")),
              "LIMIT", new ScalarArgument(IntegerType.INTEGER, 10L));
      TableFunctionAnalysis analysis =
          new GrowlerDbSearchFunction().analyze(null, null, args, null);

      Descriptor descriptor = analysis.getReturnedType().orElseThrow();
      List<Descriptor.Field> fields = descriptor.getFields();
      assertEquals(3, fields.size());
      assertEquals("day", fields.get(0).getName().orElseThrow());
      assertEquals(BigintType.BIGINT, fields.get(0).getType().orElseThrow());
      assertEquals("id", fields.get(1).getName().orElseThrow());
      assertEquals(VarcharType.VARCHAR, fields.get(1).getType().orElseThrow());
      assertEquals(SearchColumns.SCORE_COLUMN, fields.get(2).getName().orElseThrow());
      assertEquals(DoubleType.DOUBLE, fields.get(2).getType().orElseThrow());

      // --- execute: the worker re-runs the query and emits a Page matching that schema.
      GrowlerDbSearchHandle handle = (GrowlerDbSearchHandle) analysis.getHandle();
      GrowlerDbSearchProcessor processor = new GrowlerDbSearchProcessor(handle);

      TableFunctionProcessorState state = processor.process();
      assertTrue(state instanceof TableFunctionProcessorState.Processed);
      Page page = ((TableFunctionProcessorState.Processed) state).getResult();
      assertEquals(2, page.getPositionCount());

      Block dayBlock = page.getBlock(0);
      Block idBlock = page.getBlock(1);
      Block scoreBlock = page.getBlock(2);
      assertEquals(20L, BigintType.BIGINT.getLong(dayBlock, 0));
      assertEquals("d1", VarcharType.VARCHAR.getSlice(idBlock, 0).toStringUtf8());
      assertEquals(1.5, DoubleType.DOUBLE.getDouble(scoreBlock, 0));
      assertEquals("d2", VarcharType.VARCHAR.getSlice(idBlock, 1).toStringUtf8());
      assertEquals(0.5, DoubleType.DOUBLE.getDouble(scoreBlock, 1));

      // ...and the split is then finished (one page per split).
      assertEquals(TableFunctionProcessorState.Finished.FINISHED, processor.process());
    } finally {
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }
}
