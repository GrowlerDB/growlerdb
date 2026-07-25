package io.growlerdb.connector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The connector-side echo of a variant column's resolved mapping (D47/D48) — what {@link
 * VariantExtractor} needs to turn a row's variant value into wire leaves + shaped typed values.
 *
 * <p>Mirrors {@code growlerdb_core::ResolvedVariant}: the flatten flags, the discriminator path
 * (dotted, relative to the variant column, or a sibling column — resolved to a value by the
 * caller), and the declared shapes. A row whose discriminator matches no shape's {@link
 * Shape#when} skips typed extraction (counted by the caller, D45) but stays flatten-covered.
 */
public final class VariantSpec {

  /** The variant column name, e.g. {@code payload}. */
  public final String column;
  /**
   * The discriminator path (dotted). Either a sibling top-level column (e.g. {@code event_type})
   * or a path inside the variant qualified with the column (e.g. {@code payload.type}); the caller
   * resolves it to a per-row value. {@code null} for a flatten-only column.
   */
  public final String discriminator;
  /** Index every leaf as an exact {@code path = value} flatten term. */
  public final boolean flattenTerms;
  /** Feed string leaves to the analyzed catch-all (only affects which leaves matter downstream). */
  public final boolean flattenText;
  /** The declared shapes (may be empty for a flatten-only column). */
  public final List<Shape> shapes;

  public VariantSpec(
      String column,
      String discriminator,
      boolean flattenTerms,
      boolean flattenText,
      List<Shape> shapes) {
    this.column = column;
    this.discriminator = discriminator;
    this.flattenTerms = flattenTerms;
    this.flattenText = flattenText;
    this.shapes = shapes;
  }

  /** One declared shape: its name, the discriminator values that select it, and its typed paths. */
  public static final class Shape {
    public final String name;
    public final Set<String> when;
    public final List<Path> paths;

    public Shape(String name, Set<String> when, List<Path> paths) {
      this.name = name;
      this.when = when;
      this.paths = paths;
    }
  }

  /** One shaped path: its dotted path <b>relative to the variant column</b> and its declared type. */
  public static final class Path {
    public final String path;
    public final Type type;

    public Path(String path, Type type) {
      this.path = path;
      this.type = type;
    }
  }

  /** The declared scalar type of a shaped path (VECTOR paths are embedded node-side, not here). */
  public enum Type {
    TEXT,
    KEYWORD,
    LONG,
    DOUBLE,
    BOOL,
    DATE,
    IP
  }

  /**
   * Parse a compact JSON spec (the connector's {@code --variant-spec} arg) into a {@link
   * VariantSpec}. Shape of the JSON (types name the scalar {@link Type}; a shape's VECTOR paths are
   * omitted — the node embeds them from their shaped source text):
   *
   * <pre>{@code
   * {"column":"payload","discriminator":"event_type",
   *  "flatten":{"terms":true,"text":true},
   *  "shapes":[{"name":"pr","when":["PullRequestEvent"],
   *             "paths":[{"path":"number","type":"LONG"},{"path":"title","type":"TEXT"}]}]}
   * }</pre>
   *
   * {@code flatten} is optional (defaults to terms-on); {@code discriminator}/{@code shapes} are
   * optional for a flatten-only column.
   */
  public static VariantSpec fromJson(String json) {
    JsonNode root;
    try {
      root = new ObjectMapper().readTree(json);
    } catch (Exception e) {
      throw new IllegalArgumentException("invalid --variant-spec JSON: " + e.getMessage(), e);
    }
    String column = text(root, "column");
    if (column == null) {
      throw new IllegalArgumentException("--variant-spec requires a \"column\"");
    }
    String discriminator = text(root, "discriminator");
    JsonNode flatten = root.get("flatten");
    boolean terms = flatten == null || boolField(flatten, "terms", true);
    boolean text = flatten != null && boolField(flatten, "text", false);

    List<Shape> shapes = new ArrayList<>();
    JsonNode shapesNode = root.get("shapes");
    if (shapesNode != null && shapesNode.isArray()) {
      for (JsonNode s : shapesNode) {
        String name = text(s, "name");
        Set<String> when = new LinkedHashSet<>();
        JsonNode whenNode = s.get("when");
        if (whenNode != null && whenNode.isArray()) {
          whenNode.forEach(w -> when.add(w.asText()));
        } else if (whenNode != null) {
          when.add(whenNode.asText());
        }
        List<Path> paths = new ArrayList<>();
        JsonNode pathsNode = s.get("paths");
        if (pathsNode != null && pathsNode.isArray()) {
          for (JsonNode p : pathsNode) {
            paths.add(new Path(text(p, "path"), Type.valueOf(text(p, "type").toUpperCase())));
          }
        }
        shapes.add(new Shape(name, when, paths));
      }
    }
    return new VariantSpec(column, discriminator, terms, text, shapes);
  }

  private static String text(JsonNode node, String field) {
    JsonNode v = node == null ? null : node.get(field);
    return v == null || v.isNull() ? null : v.asText();
  }

  private static boolean boolField(JsonNode node, String field, boolean dflt) {
    JsonNode v = node.get(field);
    return v == null ? dflt : v.asBoolean(dflt);
  }
}
