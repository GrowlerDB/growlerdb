package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.growlerdb.proto.v1.Value;
import io.growlerdb.proto.v1.WindowingConfig;
import org.junit.jupiter.api.Test;

/**
 * Parity vectors asserting {@link WindowRouter} computes window ids byte-identically to the Rust
 * engine ({@code growlerdb_core::TimeWindowing::window_of ∘ field_micros}). If the connector and
 * engine ever disagree, a row would be streamed to a different node than the one the CP placed its
 * window on — so this guards the routing contract (task-219).
 */
class WindowRouterParityTest {

  private static final long DAY = 86_400_000_000L; // one day in canonical micros

  private static WindowRouter router(String format) {
    return new WindowRouter(
        WindowingConfig.newBuilder()
            .setField("ts")
            .setGranularity("daily")
            .setFieldFormat(format)
            .build());
  }

  private static Value intv(long i) {
    return Value.newBuilder().setInt(i).build();
  }

  private static Value ts(long micros) {
    return Value.newBuilder().setTsMicros(micros).build();
  }

  @Test
  void dailyBucketsAlignLikeTheEngine() {
    // Rust window.rs `buckets_align_to_the_window`: 2021-01-01T00:00:00Z = 1_609_459_200_000_000 µs.
    long day0 = 1_609_459_200_000_000L;
    WindowRouter us = router("epoch_micros");
    assertEquals(day0, us.windowOf(intv(day0)));
    assertEquals(day0, us.windowOf(intv(day0 + 1)));
    assertEquals(day0, us.windowOf(intv(day0 + DAY - 1)));
    assertEquals(day0 + DAY, us.windowOf(intv(day0 + DAY)));
  }

  @Test
  void numericEpochsNormalizeToMicros() {
    long day0 = 1_609_459_200_000_000L;
    // epoch_seconds → micros, then bucket.
    assertEquals(day0, router("epoch_seconds").windowOf(intv(1_609_459_200L)));
    // epoch_millis, mirroring window.rs `a_format_declared_millis_window_field_buckets_in_micros`:
    // day-10 millis + 5 → the day-10 micros window.
    long dayMs10 = 10L * 86_400_000L;
    assertEquals(10L * DAY, router("epoch_millis").windowOf(intv(dayMs10 + 5)));
    // epoch_nanos truncates to micros.
    assertEquals(day0, router("epoch_nanos").windowOf(intv(day0 * 1_000L + 999L)));
  }

  @Test
  void nativeAndPreNormalizedTimestamps() {
    long day0 = 1_609_459_200_000_000L;
    // Native field (format ""): a raw Int is already micros; a Ts (unexpected here) → window 0,
    // exactly as the engine's field_micros(None) returns None → unwrap_or(0).
    assertEquals(day0, router("").windowOf(intv(day0)));
    assertEquals(0L, router("").windowOf(ts(day0)));
    // With a declared format, a pre-normalized timestamp short-circuits to its micros (to_micros).
    assertEquals(day0, router("epoch_seconds").windowOf(ts(day0)));
  }

  @Test
  void stringDateFormatIsALoudErrorNotASilentMisroute() {
    // String date formats aren't supported for connector-side routing yet — fail loudly rather than
    // silently route to window 0 (which would mismatch the engine).
    assertThrows(IllegalArgumentException.class, () -> router("rfc3339").windowOf(intv(1)));
  }
}
