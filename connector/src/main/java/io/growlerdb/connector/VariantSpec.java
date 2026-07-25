package io.growlerdb.connector;

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
  /** Index every leaf as an exact {@code path = value} flatten term. */
  public final boolean flattenTerms;
  /** Feed string leaves to the analyzed catch-all (only affects which leaves matter downstream). */
  public final boolean flattenText;
  /** The declared shapes (may be empty for a flatten-only column). */
  public final List<Shape> shapes;

  public VariantSpec(String column, boolean flattenTerms, boolean flattenText, List<Shape> shapes) {
    this.column = column;
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
}
