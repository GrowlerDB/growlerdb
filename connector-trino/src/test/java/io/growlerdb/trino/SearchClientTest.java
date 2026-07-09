package io.growlerdb.trino;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** {@link SearchClient} forwards the query to the Node {@code Search} RPC and returns its hits. */
class SearchClientTest {

  @Test
  void searchForwardsTheQueryAndReturnsHits() throws Exception {
    AtomicReference<SearchRequest> seen = new AtomicReference<>();
    Server server =
        ServerBuilder.forPort(0)
            .addService(
                new SearchGrpc.SearchImplBase() {
                  @Override
                  public void search(SearchRequest request, StreamObserver<SearchResponse> obs) {
                    seen.set(request);
                    SearchHit hit =
                        SearchHit.newBuilder()
                            .setCoordinates(
                                Coordinates.newBuilder()
                                    .addIdentifier(
                                        Field.newBuilder()
                                            .setName("id")
                                            .setValue(Value.newBuilder().setStr("d1"))))
                            .setScore(1.5)
                            .build();
                    obs.onNext(SearchResponse.newBuilder().addHits(hit).build());
                    obs.onCompleted();
                  }
                })
            .build()
            .start();
    try (SearchClient client = new SearchClient("127.0.0.1", server.getPort())) {
      List<SearchHit> hits = client.search("body:error", 25);
      assertEquals("body:error", seen.get().getQuery());
      assertEquals(25, seen.get().getLimit());
      assertEquals(1, hits.size());
      assertEquals("d1", hits.get(0).getCoordinates().getIdentifier(0).getValue().getStr());
    } finally {
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }
}
