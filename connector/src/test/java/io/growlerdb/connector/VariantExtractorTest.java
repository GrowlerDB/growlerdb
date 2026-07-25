package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.growlerdb.proto.v1.Value;
import io.growlerdb.proto.v1.VariantColumn;
import io.growlerdb.proto.v1.VariantLeaf;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the pure variant walk (D47/D48, TASK-350/352) — flatten leaf extraction, shaped
 * typed extraction selected by the discriminator, and the unmatched-discriminator path. No Spark.
 */
class VariantExtractorTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** `payload` VARIANT: flatten (terms) + two shapes (pr/issue) selected by `event_type`. */
  private static VariantSpec eventsSpec() {
    VariantSpec.Shape pr =
        new VariantSpec.Shape(
            "pull_request",
            Set.of("PullRequestEvent"),
            List.of(
                new VariantSpec.Path("number", VariantSpec.Type.LONG),
                new VariantSpec.Path("action", VariantSpec.Type.KEYWORD),
                new VariantSpec.Path("merged", VariantSpec.Type.BOOL)));
    VariantSpec.Shape issue =
        new VariantSpec.Shape(
            "issue",
            Set.of("IssuesEvent"),
            List.of(new VariantSpec.Path("number", VariantSpec.Type.LONG)));
    return new VariantSpec("payload", true, true, List.of(pr, issue));
  }

  private static JsonNode json(String s) {
    try {
      return MAPPER.readTree(s);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** All flatten leaves for `path` (a path may repeat for array elements). */
  private static List<Value> leaves(VariantColumn col, String path) {
    return col.getLeavesList().stream()
        .filter(l -> l.getPath().equals(path))
        .map(VariantLeaf::getValue)
        .toList();
  }

  @Test
  void flattenWalksEveryLeafColumnQualifiedIncludingNestedAndArrays() {
    var extractor = new VariantExtractor(eventsSpec());
    JsonNode payload =
        json(
            "{\"action\":\"opened\",\"number\":1347,\"merged\":false,"
                + "\"user\":{\"login\":\"octocat\",\"id\":583231},"
                + "\"labels\":[\"bug\",\"ci\"]}");
    VariantExtractor.Extracted e = extractor.extract(payload, "PullRequestEvent");
    VariantColumn col = e.variantColumn;

    assertEquals("payload", col.getField());
    assertEquals("PullRequestEvent", col.getDiscriminator());
    // Scalars flatten column-qualified, typed (int/str/bool), incl. nested `user.*`.
    assertEquals(1347L, leaves(col, "payload.number").get(0).getInt());
    assertEquals("octocat", leaves(col, "payload.user.login").get(0).getStr());
    assertEquals(583231L, leaves(col, "payload.user.id").get(0).getInt());
    assertFalse(leaves(col, "payload.merged").get(0).getBool());
    // Array scalars flatten as repeated same-path leaves (so `payload.labels:bug` matches).
    List<Value> labels = leaves(col, "payload.labels");
    assertEquals(2, labels.size());
    assertEquals(Set.of("bug", "ci"), Set.of(labels.get(0).getStr(), labels.get(1).getStr()));
  }

  @Test
  void discriminatorSelectsShapeAndExtractsTypedValues() {
    var extractor = new VariantExtractor(eventsSpec());
    JsonNode payload = json("{\"action\":\"closed\",\"number\":1290,\"merged\":true}");
    VariantExtractor.Extracted e = extractor.extract(payload, "PullRequestEvent");

    assertFalse(e.unmatchedDiscriminator);
    Map<String, Value> shaped = e.shapedFields;
    // Declared paths ride the normal `fields` map at their dotted column.path, typed.
    assertEquals(1290L, shaped.get("payload.number").getInt());
    assertEquals("closed", shaped.get("payload.action").getStr());
    assertTrue(shaped.get("payload.merged").getBool());
  }

  @Test
  void unmatchedDiscriminatorSkipsShapesButStaysFlattenCovered() {
    var extractor = new VariantExtractor(eventsSpec());
    // `WatchEvent` matches neither shape → typed extraction skipped, counter flag set...
    JsonNode payload = json("{\"action\":\"started\",\"user\":{\"login\":\"octocat\"}}");
    VariantExtractor.Extracted e = extractor.extract(payload, "WatchEvent");

    assertTrue(e.unmatchedDiscriminator, "no shape matched → counted by the caller (D45)");
    assertTrue(e.shapedFields.isEmpty(), "no typed extraction");
    // ...but flatten still covers the whole value.
    assertEquals("started", leaves(e.variantColumn, "payload.action").get(0).getStr());
    assertEquals("octocat", leaves(e.variantColumn, "payload.user.login").get(0).getStr());
  }

  @Test
  void issueShapeExtractsOnlyItsDeclaredPaths() {
    var extractor = new VariantExtractor(eventsSpec());
    JsonNode payload = json("{\"action\":\"opened\",\"number\":88,\"labels\":[\"bug\"]}");
    VariantExtractor.Extracted e = extractor.extract(payload, "IssuesEvent");

    assertFalse(e.unmatchedDiscriminator);
    // The `issue` shape declares only `number` → `action` is not a typed field here (flatten only).
    assertEquals(Set.of("payload.number"), e.shapedFields.keySet());
    assertEquals(88L, e.shapedFields.get("payload.number").getInt());
  }
}
