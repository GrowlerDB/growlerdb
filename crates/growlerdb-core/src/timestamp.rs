//! Timestamp parsing: turn a source value into GrowlerDB's **canonical internal
//! representation — epoch microseconds** (the unit Tantivy's `DateTime::from_timestamp_micros` and
//! Iceberg's default `timestamp` precision use). A field declared with a [`TimeFormat`] in the index
//! definition is treated as a `DATE` regardless of its source Arrow type, so a plain `int64` epoch
//! column (the streaming demo's `ts`, in millis) or a digit string can be a real timestamp — driving
//! range queries, sort, the console time filter, window pruning, and date
//! histograms on one consistent scale.
//!
//! Parsing is **loud, not silent**: a wrong value type, an unparseable string, or an
//! overflow is a clear error, never an off-by-10³ or off-by-timezone date.
//!
//! Two families of format are supported: **integer-epoch units** (`epoch_seconds`/`millis`/`micros`/
//! `nanos` — what the demo and most lake tables use) and **string formats** (`rfc3339` for an
//! ISO-8601 datetime carrying an offset, and `date_only` for a `YYYY-MM-DD` calendar date, taken at
//! UTC midnight).

use crate::Value;

/// How a source column encodes a timestamp. Declared per field in the index definition; converted
/// to canonical **epoch microseconds** by [`to_micros`](TimeFormat::to_micros).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeFormat {
    /// Integer seconds since the Unix epoch.
    #[serde(alias = "epoch_s")]
    EpochSeconds,
    /// Integer milliseconds since the Unix epoch (e.g. the demo's `ts`).
    #[serde(alias = "epoch_ms")]
    EpochMillis,
    /// Integer microseconds since the Unix epoch — already canonical.
    #[serde(alias = "epoch_us")]
    EpochMicros,
    /// Integer nanoseconds since the Unix epoch (truncated to micros).
    #[serde(alias = "epoch_ns")]
    EpochNanos,
    /// An **RFC3339 / ISO-8601** datetime *string* carrying an offset, e.g. `2026-06-29T12:30:00Z`
    /// or `2026-06-29T14:30:00+02:00`. Normalized to UTC micros.
    #[serde(alias = "iso8601")]
    Rfc3339,
    /// A **date-only** string `YYYY-MM-DD` (e.g. `2026-06-29`), taken at **UTC midnight**.
    #[serde(alias = "date")]
    DateOnly,
}

/// A timestamp value that could not be parsed to canonical micros — surfaced as a build/ingest
/// error rather than silently producing a wrong date.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TimeParseError {
    /// The value wasn't an integer (nor a digit string) for an integer-epoch format.
    #[error("field `{path}`: expected an integer epoch ({format:?}) but got `{value}`")]
    NotInteger {
        path: String,
        format: TimeFormat,
        value: String,
    },
    /// A string-format value (`Rfc3339`/`DateOnly`) didn't parse as that format.
    #[error("field `{path}`: could not parse `{value}` as {format:?}")]
    NotParsable {
        path: String,
        format: TimeFormat,
        value: String,
    },
    /// Converting to micros overflowed `i64`.
    #[error("field `{path}`: epoch value {value} ({format:?}) overflows i64 microseconds")]
    Overflow {
        path: String,
        format: TimeFormat,
        value: i64,
    },
}

impl TimeFormat {
    /// Convert a source [`Value`] to **canonical epoch microseconds** for `path` (used only in the
    /// error message). For an epoch unit, accepts an integer or a digit string (common in JSON-ish
    /// sources); for a string format, accepts the matching `YYYY-MM-DD[THH:MM:SS±ZZ]` text. Anything
    /// else, an unparseable string, or an overflow is a loud [`TimeParseError`].
    pub fn to_micros(self, path: &str, value: &Value) -> Result<i64, TimeParseError> {
        // A `Ts` is *already* canonical micros by definition (a native source date/timestamp
        // normalized at extraction) — a declared format describes some *other* source shape, so
        // it must not rescale a value that arrives pre-normalized.
        if let Value::Ts(micros) = value {
            return Ok(*micros);
        }
        match self {
            TimeFormat::EpochSeconds
            | TimeFormat::EpochMillis
            | TimeFormat::EpochMicros
            | TimeFormat::EpochNanos => self.epoch_to_micros(path, value),
            TimeFormat::Rfc3339 | TimeFormat::DateOnly => self.string_to_micros(path, value),
        }
    }

    /// Integer-epoch path: pull an `i64` out of the value (int or digit string), then scale to micros.
    fn epoch_to_micros(self, path: &str, value: &Value) -> Result<i64, TimeParseError> {
        let n = match value {
            Value::Int(i) => *i,
            Value::Str(s) => s
                .trim()
                .parse::<i64>()
                .map_err(|_| TimeParseError::NotInteger {
                    path: path.to_string(),
                    format: self,
                    value: s.clone(),
                })?,
            other => {
                return Err(TimeParseError::NotInteger {
                    path: path.to_string(),
                    format: self,
                    value: other.to_index_string(),
                })
            }
        };
        let overflow = || TimeParseError::Overflow {
            path: path.to_string(),
            format: self,
            value: n,
        };
        match self {
            TimeFormat::EpochSeconds => n.checked_mul(1_000_000).ok_or_else(overflow),
            TimeFormat::EpochMillis => n.checked_mul(1_000).ok_or_else(overflow),
            TimeFormat::EpochMicros => Ok(n),
            // Nanos → micros floors to the microsecond (canonical is micros). `div_euclid` (not `/`)
            // so a pre-1970 nanos value floors consistently with the event/window bucketing, which
            // also uses `div_euclid` — avoids a sign-dependent 1µs mismatch.
            TimeFormat::EpochNanos => Ok(n.div_euclid(1_000)),
            // Unreachable: only the four epoch units route here.
            TimeFormat::Rfc3339 | TimeFormat::DateOnly => unreachable!(),
        }
    }

    /// String path: parse RFC3339 (offset-aware) or a `YYYY-MM-DD` date at UTC midnight → micros.
    fn string_to_micros(self, path: &str, value: &Value) -> Result<i64, TimeParseError> {
        let bad = |v: &str| TimeParseError::NotParsable {
            path: path.to_string(),
            format: self,
            value: v.to_string(),
        };
        let Value::Str(s) = value else {
            return Err(bad(&value.to_index_string()));
        };
        let s = s.trim();
        let micros = match self {
            TimeFormat::Rfc3339 => chrono::DateTime::parse_from_rfc3339(s)
                .map_err(|_| bad(s))?
                .timestamp_micros(),
            TimeFormat::DateOnly => chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|_| bad(s))?
                .and_hms_opt(0, 0, 0)
                .expect("midnight is a valid time")
                .and_utc()
                .timestamp_micros(),
            _ => unreachable!("only string formats route here"),
        };
        Ok(micros)
    }
}

/// Parse a **query range bound** on a DATE field to canonical **epoch micros**. The index stores
/// DATE columns as canonical micros, so a raw integer is accepted verbatim; for authoring
/// convenience a bound may also be written as an **ISO-8601 / RFC3339** datetime (`2024-01-01T00:00:00Z`,
/// offset-aware) or a bare `YYYY-MM-DD` calendar date (taken at UTC midnight). Returns `None` if the
/// string is none of these — the caller turns that into a loud query-type error.
pub fn parse_date_query_bound(s: &str) -> Option<i64> {
    let s = s.trim();
    // Canonical micros: a bare integer is the stored unit — accept it verbatim (keeps existing
    // epoch-micros queries working).
    if let Ok(micros) = s.parse::<i64>() {
        return Some(micros);
    }
    // RFC3339 / ISO-8601 datetime carrying an offset.
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp_micros());
    }
    // Bare calendar date `YYYY-MM-DD` at UTC midnight.
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Some(
            d.and_hms_opt(0, 0, 0)
                .expect("midnight is a valid time")
                .and_utc()
                .timestamp_micros(),
        );
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date_query_bounds_accept_micros_rfc3339_and_bare_dates() {
        // Raw canonical micros pass through unchanged.
        assert_eq!(
            parse_date_query_bound("1782691200000000"),
            Some(1_782_691_200_000_000)
        );
        // A bare `YYYY-MM-DD` lands on UTC midnight.
        assert_eq!(
            parse_date_query_bound("2026-06-29"),
            Some(1_782_691_200_000_000)
        );
        // An RFC3339 datetime (offset-aware) → the same UTC micros.
        assert_eq!(
            parse_date_query_bound("2026-06-29T00:00:00Z"),
            Some(1_782_691_200_000_000)
        );
        assert_eq!(
            parse_date_query_bound("2026-06-29T02:00:00+02:00"),
            Some(1_782_691_200_000_000)
        );
        // Garbage is a loud `None`.
        assert_eq!(parse_date_query_bound("not-a-date"), None);
    }

    #[test]
    fn integer_epochs_convert_to_canonical_micros() {
        // 2026-06-29T00:00:00Z ≈ 1_782_000_000 s; the same instant across units → the same micros.
        assert_eq!(
            TimeFormat::EpochSeconds.to_micros("ts", &Value::Int(1_782_000_000)),
            Ok(1_782_000_000_000_000)
        );
        assert_eq!(
            TimeFormat::EpochMillis.to_micros("ts", &Value::Int(1_782_000_000_000)),
            Ok(1_782_000_000_000_000)
        );
        assert_eq!(
            TimeFormat::EpochMicros.to_micros("ts", &Value::Int(1_782_000_000_000_000)),
            Ok(1_782_000_000_000_000)
        );
        assert_eq!(
            TimeFormat::EpochNanos.to_micros("ts", &Value::Int(1_782_000_000_000_000_999)),
            Ok(1_782_000_000_000_000) // sub-micro truncated
        );
    }

    #[test]
    fn negative_pre_1970_epochs_are_fine() {
        assert_eq!(
            TimeFormat::EpochMillis.to_micros("ts", &Value::Int(-1000)),
            Ok(-1_000_000)
        );
    }

    #[test]
    fn pre_1970_nanos_floor_consistently_with_windowing() {
        // Nanos→micros uses `div_euclid`, so a pre-epoch value floors (like the event/
        // window bucketing) rather than truncating toward zero (which `/` would do → -1µs, an
        // off-by-one vs the window field).
        assert_eq!(
            TimeFormat::EpochNanos.to_micros("ts", &Value::Int(-1_500)),
            Ok(-2) // floor(-1.5µs), not -1
        );
    }

    #[test]
    fn an_epoch_carried_as_a_digit_string_parses() {
        assert_eq!(
            TimeFormat::EpochMillis.to_micros("ts", &Value::Str("1782000000000".into())),
            Ok(1_782_000_000_000_000)
        );
    }

    #[test]
    fn a_non_integer_value_is_a_loud_error_not_a_wrong_date() {
        assert!(matches!(
            TimeFormat::EpochMillis.to_micros("ts", &Value::Str("not-a-date".into())),
            Err(TimeParseError::NotInteger { .. })
        ));
        assert!(matches!(
            TimeFormat::EpochSeconds.to_micros("ts", &Value::Bool(true)),
            Err(TimeParseError::NotInteger { .. })
        ));
    }

    #[test]
    fn an_overflowing_epoch_is_an_error() {
        assert!(matches!(
            TimeFormat::EpochSeconds.to_micros("ts", &Value::Int(i64::MAX)),
            Err(TimeParseError::Overflow { .. })
        ));
    }

    #[test]
    fn the_format_deserializes_from_canonical_and_alias_names() {
        let from = |s: &str| serde_json::from_str::<TimeFormat>(s).unwrap();
        assert_eq!(from("\"epoch_millis\""), TimeFormat::EpochMillis);
        assert_eq!(from("\"epoch_ms\""), TimeFormat::EpochMillis); // alias
        assert_eq!(from("\"epoch_s\""), TimeFormat::EpochSeconds);
        assert_eq!(from("\"rfc3339\""), TimeFormat::Rfc3339);
        assert_eq!(from("\"iso8601\""), TimeFormat::Rfc3339); // alias
        assert_eq!(from("\"date_only\""), TimeFormat::DateOnly);
        assert_eq!(from("\"date\""), TimeFormat::DateOnly); // alias
    }

    #[test]
    fn rfc3339_strings_parse_to_utc_micros_regardless_of_offset() {
        // 2026-06-29T00:00:00Z == 1_782_691_200 s → ×1e6 micros.
        let utc = TimeFormat::Rfc3339.to_micros("ts", &Value::Str("2026-06-29T00:00:00Z".into()));
        assert_eq!(utc, Ok(1_782_691_200_000_000));
        // The same instant written with a +02:00 offset normalizes to the same UTC micros.
        let offset =
            TimeFormat::Rfc3339.to_micros("ts", &Value::Str("2026-06-29T02:00:00+02:00".into()));
        assert_eq!(offset, utc);
        // Sub-second precision is kept down to micros.
        assert_eq!(
            TimeFormat::Rfc3339.to_micros("ts", &Value::Str("2026-06-29T00:00:00.000001Z".into())),
            Ok(1_782_691_200_000_001)
        );
    }

    #[test]
    fn a_date_only_string_lands_on_utc_midnight() {
        assert_eq!(
            TimeFormat::DateOnly.to_micros("ts", &Value::Str("2026-06-29".into())),
            Ok(1_782_691_200_000_000) // 2026-06-29T00:00:00Z
        );
    }

    #[test]
    fn unparseable_or_wrong_typed_strings_are_loud_errors() {
        // A non-RFC3339 string (this is a bare date, not a datetime).
        assert!(matches!(
            TimeFormat::Rfc3339.to_micros("ts", &Value::Str("2026-06-29".into())),
            Err(TimeParseError::NotParsable { .. })
        ));
        // A garbage date-only string.
        assert!(matches!(
            TimeFormat::DateOnly.to_micros("ts", &Value::Str("June 29".into())),
            Err(TimeParseError::NotParsable { .. })
        ));
        // A string format handed a non-string value.
        assert!(matches!(
            TimeFormat::Rfc3339.to_micros("ts", &Value::Int(1_782_000_000)),
            Err(TimeParseError::NotParsable { .. })
        ));
    }
}
