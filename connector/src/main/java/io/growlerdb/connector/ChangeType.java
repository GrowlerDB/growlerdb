package io.growlerdb.connector;

/**
 * Iceberg changelog row change type (the {@code _change_type} column). A
 * changelog reduces to upserts and deletes by composite key
 * (<a href="../../../../wiki/06-ingestion.md">CDC from Iceberg</a>).
 */
public enum ChangeType {
  INSERT,
  DELETE,
  UPDATE_BEFORE,
  UPDATE_AFTER;

  /** Parse Iceberg's {@code _change_type} string. */
  public static ChangeType fromIceberg(String value) {
    return switch (value) {
      case "INSERT" -> INSERT;
      case "DELETE" -> DELETE;
      case "UPDATE_BEFORE" -> UPDATE_BEFORE;
      case "UPDATE_AFTER" -> UPDATE_AFTER;
      default -> throw new IllegalArgumentException("unknown _change_type: " + value);
    };
  }

  /** INSERT / UPDATE_AFTER → index the (new) version. */
  public boolean isUpsert() {
    return this == INSERT || this == UPDATE_AFTER;
  }

  /** DELETE / UPDATE_BEFORE → remove the prior version. */
  public boolean isDelete() {
    return this == DELETE || this == UPDATE_BEFORE;
  }
}
