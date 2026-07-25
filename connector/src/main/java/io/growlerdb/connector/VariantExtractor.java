package io.growlerdb.connector;

import com.fasterxml.jackson.databind.JsonNode;
import io.growlerdb.proto.v1.Value;
import io.growlerdb.proto.v1.VariantColumn;
import io.growlerdb.proto.v1.VariantLeaf;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Extraction-time variant support (D47/D48, TASK-350/352) — the connector-side walk that turns one
 * row's variant value into the wire shapes the node indexes, keeping the wire scalar-leaf-only.
 *
 * <p>Given a variant value (Spark reads it and hands us its JSON via {@code VariantVal.toJson()};
 * here a parsed Jackson {@link JsonNode}) and its {@link VariantSpec}, {@link #extract} produces:
 *
 * <ul>
 *   <li>a {@link VariantColumn} of untyped <b>flatten</b> leaves — every scalar leaf as a
 *       column-qualified dotted path + its scalar {@link Value} (the node indexes {@code
 *       pathvalue} terms + a string catch-all), plus the row's discriminator value;
 *   <li>a map of declared <b>shape</b> paths → typed {@link Value}, extracted via {@code
 *       variant_get} on the shape selected by the discriminator — these ride the document's normal
 *       {@code fields} map at their dotted {@code column.path}.
 * </ul>
 *
 * <p>A row whose discriminator matches no shape yields {@link Extracted#unmatchedDiscriminator}
 * (the caller bumps a D45 ingest counter) and no shaped values — but is still fully flatten-covered.
 * Array leaves flatten as repeated same-path terms (so {@code labels:bug} matches an element).
 */
public final class VariantExtractor {

  private final VariantSpec spec;

  public VariantExtractor(VariantSpec spec) {
    this.spec = spec;
  }

  /** The spec this extractor was built from (the caller reads its column + discriminator). */
  public VariantSpec spec() {
    return spec;
  }

  /** The result of extracting one row's variant value. */
  public static final class Extracted {
    /** The flatten leaves + discriminator for the wire (empty leaves if flatten is off). */
    public final VariantColumn variantColumn;
    /** Declared shape paths → typed value, keyed by dotted {@code column.path}. */
    public final Map<String, Value> shapedFields;
    /** True when the discriminator matched no declared shape (typed extraction skipped). */
    public final boolean unmatchedDiscriminator;

    Extracted(VariantColumn variantColumn, Map<String, Value> shapedFields, boolean unmatched) {
      this.variantColumn = variantColumn;
      this.shapedFields = shapedFields;
      this.unmatchedDiscriminator = unmatched;
    }
  }

  /**
   * Extract from a row's variant {@code value} with the row's {@code discriminatorValue} (already
   * resolved by the caller — from inside the variant or a sibling column). A {@code null}/missing
   * value yields an empty extraction.
   */
  public Extracted extract(JsonNode value, String discriminatorValue) {
    VariantColumn.Builder col = VariantColumn.newBuilder().setField(spec.column);
    if (discriminatorValue != null) {
      col.setDiscriminator(discriminatorValue);
    }
    // Flatten: walk every leaf into a column-qualified path=value term.
    if ((spec.flattenTerms || spec.flattenText) && value != null && !value.isNull()) {
      walk(spec.column, value, col);
    }

    // Shapes: the discriminator selects at most one shape; extract its declared paths via a
    // path walk (the connector-side analogue of Spark's variant_get on the selected shape).
    Map<String, Value> shaped = new LinkedHashMap<>();
    boolean matched = false;
    if (!spec.shapes.isEmpty() && discriminatorValue != null) {
      for (VariantSpec.Shape shape : spec.shapes) {
        if (!shape.when.contains(discriminatorValue)) {
          continue;
        }
        matched = true;
        for (VariantSpec.Path p : shape.paths) {
          JsonNode at = getPath(value, p.path);
          if (at == null || at.isNull()) {
            continue; // a declared path absent for this row — dropped (counted by the caller)
          }
          Value typed = typed(at, p.type);
          if (typed != null) {
            shaped.put(spec.column + "." + p.path, typed);
          }
        }
        break; // first matching shape wins (discriminator is 1:1)
      }
    }
    boolean unmatched = !spec.shapes.isEmpty() && !matched;
    return new Extracted(col.build(), shaped, unmatched);
  }

  /** Recurse `node` under column-qualified `path`, appending a flatten leaf per scalar. */
  private void walk(String path, JsonNode node, VariantColumn.Builder col) {
    if (node.isObject()) {
      node.fields()
          .forEachRemaining(e -> walk(path + "." + e.getKey(), e.getValue(), col));
    } else if (node.isArray()) {
      // Array scalar elements flatten as repeated same-path leaves (multi-valued); nested
      // objects/arrays recurse under the same path.
      node.forEach(child -> walk(path, child, col));
    } else if (!node.isNull()) {
      Value v = scalar(node);
      if (v != null) {
        col.addLeaves(VariantLeaf.newBuilder().setPath(path).setValue(v).build());
      }
    }
  }

  /** Navigate a dotted `path` (relative to the column) into `root`; `null` if any segment misses. */
  private static JsonNode getPath(JsonNode root, String path) {
    JsonNode cur = root;
    for (String seg : path.split("\\.")) {
      if (cur == null || !cur.isObject()) {
        return null;
      }
      cur = cur.get(seg);
    }
    return cur;
  }

  /** Map a JSON scalar to an untyped flatten {@link Value} (number → int if integral, else float). */
  private static Value scalar(JsonNode n) {
    if (n.isTextual()) {
      return Value.newBuilder().setStr(n.textValue()).build();
    }
    if (n.isBoolean()) {
      return Value.newBuilder().setBool(n.booleanValue()).build();
    }
    if (n.isIntegralNumber()) {
      return Value.newBuilder().setInt(n.longValue()).build();
    }
    if (n.isNumber()) {
      return Value.newBuilder().setFloat(n.doubleValue()).build();
    }
    return null;
  }

  /** Map a JSON value to a typed shape {@link Value} per the declared {@link VariantSpec.Type}. */
  private static Value typed(JsonNode n, VariantSpec.Type type) {
    switch (type) {
      case LONG:
        return n.isNumber()
            ? Value.newBuilder().setInt(n.longValue()).build()
            : (n.isTextual() ? parseLong(n.textValue()) : null);
      case DOUBLE:
        return n.isNumber() ? Value.newBuilder().setFloat(n.doubleValue()).build() : null;
      case BOOL:
        return n.isBoolean() ? Value.newBuilder().setBool(n.booleanValue()).build() : null;
      case TEXT:
      case KEYWORD:
      case IP:
        // Text-ish types index the value's string form; a non-string leaf is stringified.
        return Value.newBuilder().setStr(n.isTextual() ? n.textValue() : n.asText()).build();
      case DATE:
        // A DATE shape path carries an epoch value; the node normalizes via the field's `format`.
        return n.isNumber() ? Value.newBuilder().setInt(n.longValue()).build() : null;
      default:
        return null;
    }
  }

  private static Value parseLong(String s) {
    try {
      return Value.newBuilder().setInt(Long.parseLong(s)).build();
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
