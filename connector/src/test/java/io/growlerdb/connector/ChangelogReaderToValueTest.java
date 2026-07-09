package io.growlerdb.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.growlerdb.proto.v1.Value;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

/**
 * {@link ChangelogReader#toValue} temporal mapping (task-184): Spark date/timestamp scalars must
 * become {@code ts_micros} — canonical <b>epoch microseconds UTC</b> — matching what the Rust
 * source extracts for the same Iceberg value, so a temporal key hashes/routes identically on both
 * sides (see {@link ShardRouterParityTest} for the byte-level contract).
 */
class ChangelogReaderToValueTest {

  private static final long MICROS_PER_DAY = 86_400_000_000L;

  @Test
  void sparkDateMapsToUtcMidnightMicros() {
    LocalDate day = LocalDate.of(2026, 6, 21);
    long expected = day.toEpochDay() * MICROS_PER_DAY;
    assertEquals(expected, ChangelogReader.toValue(java.sql.Date.valueOf(day)).getTsMicros());
    assertEquals(expected, ChangelogReader.toValue(day).getTsMicros());
  }

  @Test
  void sparkTimestampMapsToEpochMicros() {
    // 1_782_000_123_456_789 µs — the same instant used by the cross-language golden vectors.
    long micros = 1_782_000_123_456_789L;
    Instant instant = Instant.ofEpochSecond(micros / 1_000_000L, (micros % 1_000_000L) * 1_000L);
    assertEquals(micros, ChangelogReader.toValue(instant).getTsMicros());
    assertEquals(
        micros, ChangelogReader.toValue(java.sql.Timestamp.from(instant)).getTsMicros());
    // TIMESTAMP_NTZ external type (LocalDateTime): taken at UTC.
    LocalDateTime ntz = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    assertEquals(micros, ChangelogReader.toValue(ntz).getTsMicros());
  }

  @Test
  void nonTemporalScalarsKeepTheirExistingKinds() {
    assertEquals("x", ChangelogReader.toValue("x").getStr());
    assertEquals(7L, ChangelogReader.toValue(7L).getInt());
    assertEquals(Value.KindCase.BOOL, ChangelogReader.toValue(true).getKindCase());
  }
}
