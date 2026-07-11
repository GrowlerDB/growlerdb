package io.growlerdb.connector;

import io.growlerdb.proto.v1.Value;
import io.growlerdb.proto.v1.WindowingConfig;

/**
 * JVM port of the engine's window routing: compute the time-window id a document falls in,
 * <b>byte-identically</b> to {@code growlerdb_core::TimeWindowing::window_of ∘ field_micros} — so the
 * connector streams each row to the same window's owning node the engine will store it in. A mismatch
 * would route a row to the wrong node (the CP placed the window on node A, but the row lands on B).
 *
 * <p><b>This must stay in lockstep with {@code crates/growlerdb-core/src/{window,timestamp}.rs}.</b>
 * {@code WindowRouterParityTest} asserts the same golden vectors both sides compute, so drift fails a
 * test rather than silently misrouting writes.
 */
public final class WindowRouter {

  private final String field;
  private final String fieldFormat;
  private final long granularityMicros;

  public WindowRouter(WindowingConfig cfg) {
    this.field = cfg.getField();
    this.fieldFormat = cfg.getFieldFormat();
    this.granularityMicros = granularityMicros(cfg.getGranularity());
  }

  /** The ingest-time field whose value places a document in a window. */
  public String field() {
    return field;
  }

  /** Window length in canonical micros — mirrors {@code WindowGranularity::micros}. */
  static long granularityMicros(String granularity) {
    return switch (granularity) {
      case "hourly" -> 3_600_000_000L;
      case "daily" -> 86_400_000_000L;
      case "weekly" -> 7L * 86_400_000_000L;
      default -> throw new IllegalArgumentException("unknown window granularity: " + granularity);
    };
  }

  /** The window id (epoch-micros of the window start) for a window-field {@code value}. */
  public long windowOf(Value value) {
    long micros = toMicros(value, fieldFormat);
    // Rust `window_of`: epoch_us.div_euclid(w) * w. For a positive divisor, div_euclid == floorDiv.
    return Math.floorDiv(micros, granularityMicros) * granularityMicros;
  }

  /**
   * Canonical epoch micros for a window value — mirrors {@code field_micros ∘ TimeFormat::to_micros}.
   * A missing/native-non-int value maps to {@code 0} (the engine's {@code unwrap_or(0)} → window 0),
   * so behavior matches exactly; string date formats aren't supported for connector-side routing yet
   * and are a loud error rather than a silent misroute.
   */
  static long toMicros(Value v, String format) {
    if (format.isEmpty()) {
      // Native field: field_micros returns a raw Int directly; anything else (incl. a pre-normalized
      // timestamp) → None → window 0.
      return (v.getKindCase() == Value.KindCase.INT) ? v.getInt() : 0L;
    }
    // A declared format: a pre-normalized timestamp short-circuits (to_micros's `Value::Ts` arm),
    // else the integer epoch is scaled to micros.
    if (v.getKindCase() == Value.KindCase.TS_MICROS) {
      return v.getTsMicros();
    }
    if (v.getKindCase() == Value.KindCase.INT) {
      long i = v.getInt();
      return switch (format) {
        case "epoch_micros" -> i;
        case "epoch_seconds" -> Math.multiplyExact(i, 1_000_000L);
        case "epoch_millis" -> Math.multiplyExact(i, 1_000L);
        case "epoch_nanos" -> i / 1_000L;
        default ->
            throw new IllegalArgumentException(
                "window field format `"
                    + format
                    + "` (a string date) is not supported for connector windowed routing yet"
                    + " — use a numeric epoch window field");
      };
    }
    return 0L; // no usable value → window 0 (matches field_micros' unwrap_or(0))
  }
}
