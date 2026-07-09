package io.growlerdb.connector;

import java.util.List;

/**
 * The index's key + field shape the connector maps changelog rows against —
 * the JVM-side echo of the resolved index definition (partition + identifier key
 * columns, and the indexed field columns to carry on an upsert).
 */
public final class IndexMapping {

  public final List<String> partitionFields;
  public final List<String> identifierFields;
  public final List<String> fields;

  public IndexMapping(
      List<String> partitionFields, List<String> identifierFields, List<String> fields) {
    this.partitionFields = partitionFields;
    this.identifierFields = identifierFields;
    this.fields = fields;
  }
}
