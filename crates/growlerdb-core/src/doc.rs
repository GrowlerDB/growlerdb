//! Runtime document model — the data the [index](crate::index_def) is built from.
//!
//! A [`Document`] is a [`CompositeKey`] (partition + identifier values) plus its
//! mapped field values. A [`DocBatch`] is a batch of them.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A scalar field value. Mirrors the source types GrowlerDB maps from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Value {
    /// UTF-8 string.
    Str(String),
    /// 64-bit signed integer.
    Int(i64),
    /// 64-bit float.
    Float(f64),
    /// Boolean.
    Bool(bool),
    /// A temporal instant as canonical **epoch microseconds UTC** (the convention
    /// the index stores dates in). One variant covers both source dates
    /// and timestamps — the source schema disambiguates at the boundary.
    Ts(i64),
    /// A dense embedding vector (never a key field).
    Vector(Vec<f32>),
}

impl Value {
    /// Render the value as the string that gets tokenized/indexed. Non-string
    /// values are stringified for TEXT/KEYWORD indexing.
    pub fn to_index_string(&self) -> String {
        match self {
            Value::Str(s) => s.clone(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Bool(b) => b.to_string(),
            // Canonical micros, rendered like an Int — matches how a pre-parsed epoch
            // column is stringified for TEXT/KEYWORD indexing.
            Value::Ts(t) => t.to_string(),
            // Vectors are not text-indexed — they carry no meaningful string form.
            Value::Vector(_) => String::new(),
        }
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Str(s.to_string())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::Str(s)
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Value::Int(i)
    }
}

impl From<Vec<f32>> for Value {
    fn from(v: Vec<f32>) -> Self {
        Value::Vector(v)
    }
}

/// The composite document key: ordered partition fields + identifier
/// fields. Order is preserved so the key encodes/round-trips deterministically.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompositeKey {
    /// Partition field name → value, in key order.
    pub partition: Vec<(String, Value)>,
    /// Identifier field name → value, in key order.
    pub identifier: Vec<(String, Value)>,
}

impl CompositeKey {
    /// Build a composite key from its partition and identifier components.
    pub fn new(partition: Vec<(String, Value)>, identifier: Vec<(String, Value)>) -> Self {
        Self {
            partition,
            identifier,
        }
    }

    /// Look up a key field's value by name (partition first, then identifier).
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.partition
            .iter()
            .chain(self.identifier.iter())
            .find(|(n, _)| n == name)
            .map(|(_, v)| v)
    }

    /// Canonical, type-tagged byte encoding of the key — `partition[] ++
    /// identifier[]` — used as the `locator` / `key_to_doc` map key in the index
    /// store ([Design 08](../../../design/08-schemas.md)). Each field is encoded
    /// as `role · len(name) · name · type-tag · len(value) · value`, so lookups
    /// are **exact** and deterministic. It is **not** order-preserving across
    /// types — hash routing doesn't need it. Type tags: `1` Str, `2` Int,
    /// `3` Float, `4` Bool, `5` Ts, `6` Vector — the encoding of existing tags is
    /// frozen (it is the routing-hash input and the Tantivy delete term), so new types
    /// only ever **append** tags.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for (role, fields) in [(0u8, &self.partition), (1u8, &self.identifier)] {
            for (name, value) in fields {
                out.push(role);
                push_bytes(&mut out, name.as_bytes());
                push_value(&mut out, value);
            }
        }
        out
    }
}

/// Append a `u32` length prefix (LE) followed by `bytes`.
fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    // The length prefix is a `u32`, so a value/name ≥ 4 GiB would wrap and produce an ambiguous
    // (collision-prone) key. That's absurd for a key field, and everything else would break first —
    // but assert the invariant so it can't silently corrupt in dev/tests. The prefix width is part
    // of the on-disk key encoding, so it stays `u32` (widening would break existing keys).
    debug_assert!(
        bytes.len() <= u32::MAX as usize,
        "key component exceeds the u32 length prefix",
    );
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

/// Append a value as `type-tag · len-prefixed bytes`.
fn push_value(out: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Str(s) => {
            out.push(1);
            push_bytes(out, s.as_bytes());
        }
        Value::Int(i) => {
            out.push(2);
            push_bytes(out, &i.to_le_bytes());
        }
        Value::Float(f) => {
            // A float key isn't canonical cross-language and NaN has many bit patterns, so
            // `KeySpec::resolve` rejects Double key fields — a Float should never reach a key
            // position. Assert it here too, where the bytes are produced.
            debug_assert!(!f.is_nan(), "NaN float in a composite key (non-canonical)");
            out.push(3);
            push_bytes(out, &f.to_bits().to_le_bytes());
        }
        Value::Bool(b) => {
            out.push(4);
            push_bytes(out, &[*b as u8]);
        }
        Value::Ts(t) => {
            // Canonical epoch micros — same 8-byte LE shape as Int, under its own tag
            // so a timestamp key never collides with an integer key of the same value.
            out.push(5);
            push_bytes(out, &t.to_le_bytes());
        }
        Value::Vector(v) => {
            // A vector is never a key field, but `push_value` stays **total** (no panic):
            // tag `6`, the `f32` count, then each element as 4 LE bytes.
            out.push(6);
            let mut bytes = Vec::with_capacity(4 + v.len() * 4);
            bytes.extend_from_slice(&(v.len() as u32).to_le_bytes());
            for x in v {
                bytes.extend_from_slice(&x.to_le_bytes());
            }
            push_bytes(out, &bytes);
        }
    }
}

/// Errors from [`CompositeKey::decode`] — the bytes aren't a well-formed `encode()` output.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum KeyDecodeError {
    /// The input ended mid-component (a length prefix promised more bytes than remain).
    #[error("truncated key encoding at byte {0}")]
    Truncated(usize),
    /// An unknown role byte (only `0` partition / `1` identifier exist).
    #[error("unknown role byte {0} at byte {1}")]
    UnknownRole(u8, usize),
    /// An unknown value type tag (tags are append-only: 1 Str, 2 Int, 3 Float, 4 Bool, 5 Ts).
    #[error("unknown value tag {0} at byte {1}")]
    UnknownTag(u8, usize),
    /// A fixed-width value (Int/Float/Bool/Ts) carried the wrong byte length.
    #[error("value tag {tag} expects {expected} bytes, got {got}")]
    BadWidth {
        tag: u8,
        expected: usize,
        got: usize,
    },
    /// A field name wasn't valid UTF-8.
    #[error("field name is not UTF-8 at byte {0}")]
    BadName(usize),
    /// A Str value wasn't valid UTF-8.
    #[error("string value is not UTF-8 at byte {0}")]
    BadStr(usize),
    /// A Bool value byte other than 0/1 — it couldn't re-encode to the same bytes.
    #[error("bool value byte {0} at byte {1} (only 0/1 are valid)")]
    BadBool(u8, usize),
    /// Identifier fields came before partition fields, or roles interleaved — `encode()`
    /// always emits all partition fields first.
    #[error("role order violated at byte {0} (identifier before partition)")]
    RoleOrder(usize),
}

impl CompositeKey {
    /// Decode an [`encode`](Self::encode) output back into the key — the exact inverse of the
    /// frozen encoding. The index stores this same byte string as the hit's key, so hits/exports
    /// rebuild the key from it instead of a per-doc JSON copy. **Strict**: trailing
    /// bytes, unknown tags/roles, wrong fixed-value widths, and non-UTF-8 names all error —
    /// a stored key either round-trips exactly or fails loudly, never silently reshapes.
    pub fn decode(bytes: &[u8]) -> Result<Self, KeyDecodeError> {
        use KeyDecodeError as E;
        let take = |pos: &mut usize, n: usize| -> Result<std::ops::Range<usize>, E> {
            let start = *pos;
            let end = start.checked_add(n).ok_or(E::Truncated(start))?;
            if end > bytes.len() {
                return Err(E::Truncated(start));
            }
            *pos = end;
            Ok(start..end)
        };
        let take_len_prefixed = |pos: &mut usize| -> Result<std::ops::Range<usize>, E> {
            let r = take(pos, 4)?;
            let len = u32::from_le_bytes(bytes[r].try_into().expect("4 bytes")) as usize;
            take(pos, len)
        };

        let mut key = CompositeKey::new(Vec::new(), Vec::new());
        let mut pos = 0usize;
        let mut seen_identifier = false;
        while pos < bytes.len() {
            let role_at = pos;
            let role = bytes[take(&mut pos, 1)?][0];
            match role {
                0 if seen_identifier => return Err(E::RoleOrder(role_at)),
                0 | 1 => {}
                other => return Err(E::UnknownRole(other, role_at)),
            }
            seen_identifier |= role == 1;

            let name_at = pos;
            let name = std::str::from_utf8(&bytes[take_len_prefixed(&mut pos)?])
                .map_err(|_| E::BadName(name_at))?
                .to_string();

            let tag_at = pos;
            let tag = bytes[take(&mut pos, 1)?][0];
            let val_at = pos;
            let val = &bytes[take_len_prefixed(&mut pos)?];
            let fixed = |expected: usize| -> Result<&[u8], E> {
                if val.len() != expected {
                    return Err(E::BadWidth {
                        tag,
                        expected,
                        got: val.len(),
                    });
                }
                Ok(val)
            };
            let value = match tag {
                1 => Value::Str(
                    std::str::from_utf8(val)
                        .map_err(|_| E::BadStr(val_at))?
                        .to_string(),
                ),
                2 => Value::Int(i64::from_le_bytes(fixed(8)?.try_into().expect("8 bytes"))),
                3 => Value::Float(f64::from_bits(u64::from_le_bytes(
                    fixed(8)?.try_into().expect("8 bytes"),
                ))),
                // Strict 0|1 — any other byte would decode-then-re-encode to different
                // bytes, breaking the "round-trips exactly" contract.
                4 => match fixed(1)?[0] {
                    0 => Value::Bool(false),
                    1 => Value::Bool(true),
                    other => return Err(E::BadBool(other, val_at)),
                },
                5 => Value::Ts(i64::from_le_bytes(fixed(8)?.try_into().expect("8 bytes"))),
                other => return Err(E::UnknownTag(other, tag_at)),
            };
            if role == 0 {
                key.partition.push((name, value));
            } else {
                key.identifier.push((name, value));
            }
        }
        Ok(key)
    }
}

/// A position in the source stream that a [committed](crate::doc) index state
/// reflects — persisted so re-ingest from the same point is a no-op (exactly-once).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceCheckpoint {
    /// The Iceberg table snapshot the index reflects. `snapshot_id` is the position's
    /// **identity** — a random long, so it carries no order. `sequence_number`
    /// is the snapshot's Iceberg **data sequence number** — strictly monotone along a
    /// branch (v2 tables), the only sound way to order two checkpoints. `None` = unknown
    /// (a legacy persisted value, or a v1 table where sequence numbers don't exist):
    /// ordering falls back to exact-match semantics.
    IcebergSnapshot {
        snapshot_id: i64,
        sequence_number: Option<i64>,
    },
}

impl SourceCheckpoint {
    /// An Iceberg checkpoint with no ordering info (legacy shape / v1 table).
    pub fn iceberg(snapshot_id: i64) -> Self {
        SourceCheckpoint::IcebergSnapshot {
            snapshot_id,
            sequence_number: None,
        }
    }

    /// An Iceberg checkpoint carrying its lineage-monotone sequence number.
    pub fn iceberg_ordered(snapshot_id: i64, sequence_number: i64) -> Self {
        SourceCheckpoint::IcebergSnapshot {
            snapshot_id,
            sequence_number: Some(sequence_number),
        }
    }

    /// The snapshot id — the position's identity.
    pub fn snapshot_id(&self) -> i64 {
        let SourceCheckpoint::IcebergSnapshot { snapshot_id, .. } = self;
        *snapshot_id
    }

    /// The lineage-monotone sequence number, when known.
    pub fn sequence_number(&self) -> Option<i64> {
        let SourceCheckpoint::IcebergSnapshot {
            sequence_number, ..
        } = self;
        *sequence_number
    }

    /// Whether two checkpoints name the **same source position** (by snapshot id). This —
    /// not `==` — is the identity check: the same position may be carried with and without
    /// a sequence number across a format upgrade, and derived equality would call those
    /// different.
    pub fn same_position(&self, other: &SourceCheckpoint) -> bool {
        self.snapshot_id() == other.snapshot_id()
    }

    /// Lineage order between two checkpoints: `Equal` for the same position, else ordered
    /// by sequence number when **both** are known, else `None` — incomparable (snapshot
    /// ids are random longs), so callers must fall back to exact-match
    /// semantics rather than guess.
    pub fn lineage_cmp(&self, other: &SourceCheckpoint) -> Option<std::cmp::Ordering> {
        if self.same_position(other) {
            return Some(std::cmp::Ordering::Equal);
        }
        match (self.sequence_number(), other.sequence_number()) {
            (Some(a), Some(b)) => Some(a.cmp(&b)),
            _ => None,
        }
    }
}

/// Persisted/wire JSON compat: a checkpoint without a sequence number serializes to the
/// legacy shape (`{"iceberg_snapshot": id}`) so nothing changes on disk until sequence
/// numbers actually flow; with one it nests (`{"iceberg_snapshot": {"snapshot_id": ..,
/// "sequence_number": ..}}`). Deserialization accepts both.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CheckpointWire {
    IcebergSnapshot(IcebergWire),
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum IcebergWire {
    Legacy(i64),
    Ordered {
        snapshot_id: i64,
        sequence_number: Option<i64>,
    },
}

impl Serialize for SourceCheckpoint {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let wire = match self.sequence_number() {
            None => IcebergWire::Legacy(self.snapshot_id()),
            Some(seq) => IcebergWire::Ordered {
                snapshot_id: self.snapshot_id(),
                sequence_number: Some(seq),
            },
        };
        CheckpointWire::IcebergSnapshot(wire).serialize(s)
    }
}

impl<'de> Deserialize<'de> for SourceCheckpoint {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let CheckpointWire::IcebergSnapshot(wire) = CheckpointWire::deserialize(d)?;
        Ok(match wire {
            IcebergWire::Legacy(snapshot_id) => SourceCheckpoint::iceberg(snapshot_id),
            IcebergWire::Ordered {
                snapshot_id,
                sequence_number,
            } => SourceCheckpoint::IcebergSnapshot {
                snapshot_id,
                sequence_number,
            },
        })
    }
}

/// One document: its composite key and the mapped field values to index.
#[derive(Debug, Clone, PartialEq)]
pub struct Document {
    /// The composite key, stored and returned with each hit.
    pub key: CompositeKey,
    /// Field path → value. Only paths present in the index schema are indexed.
    pub fields: BTreeMap<String, Value>,
}

impl Document {
    /// Build a document from a key and its field values.
    pub fn new(key: CompositeKey, fields: BTreeMap<String, Value>) -> Self {
        Self { key, fields }
    }
}

/// A batch of documents to build into a segment (upserts only).
#[derive(Debug, Clone, Default)]
pub struct DocBatch {
    /// The documents in the batch.
    pub docs: Vec<Document>,
}

impl DocBatch {
    /// Build a batch from documents.
    pub fn new(docs: Vec<Document>) -> Self {
        Self { docs }
    }

    /// Number of documents in the batch.
    pub fn len(&self) -> usize {
        self.docs.len()
    }

    /// Whether the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.docs.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn composite_key_round_trips_through_json() {
        let key = CompositeKey::new(
            vec![("day".into(), Value::from("2026-06-19"))],
            vec![("id".into(), Value::from(42i64))],
        );
        let json = serde_json::to_string(&key).unwrap();
        let back: CompositeKey = serde_json::from_str(&json).unwrap();
        assert_eq!(key, back);
        assert_eq!(back.get("id"), Some(&Value::Int(42)));
        assert_eq!(back.get("missing"), None);
    }

    #[test]
    fn composite_key_decode_inverts_encode() {
        // Every value type, both roles, multi-field, unicode names, empty strings.
        let keys = [
            CompositeKey::new(
                vec![
                    ("day".into(), Value::Str("2026-06-19".into())),
                    ("région".into(), Value::Int(-42)),
                ],
                vec![
                    ("id".into(), Value::Str("a/b c".into())),
                    ("f".into(), Value::Float(-0.5)),
                    ("b".into(), Value::Bool(true)),
                    ("ts".into(), Value::Ts(1_782_000_000_000_000)),
                ],
            ),
            CompositeKey::new(vec![], vec![("id".into(), Value::Str(String::new()))]),
            CompositeKey::new(vec![], vec![]),
        ];
        for key in keys {
            let enc = key.encode();
            let back = CompositeKey::decode(&enc).unwrap();
            assert_eq!(key, back);
            // Byte-exact re-encode — the strictness contract.
            assert_eq!(back.encode(), enc);
        }
    }

    #[test]
    fn composite_key_decode_rejects_malformed_bytes() {
        let enc = CompositeKey::new(
            vec![("day".into(), Value::Str("x".into()))],
            vec![("id".into(), Value::Int(7))],
        )
        .encode();

        // Truncation mid-component fails loudly. (A cut on a component boundary is
        // indistinguishable from a validly shorter key — the format has no total-length
        // header — so byte 14, the end of the `day` component, is the one valid prefix.)
        let component_boundary = 14;
        for cut in 1..enc.len() {
            if cut == component_boundary {
                let shorter = CompositeKey::decode(&enc[..cut]).unwrap();
                assert_eq!(
                    shorter.encode(),
                    &enc[..cut],
                    "boundary prefix is a valid key"
                );
                continue;
            }
            assert!(
                CompositeKey::decode(&enc[..cut]).is_err(),
                "truncation at {cut} must error"
            );
        }
        // Unknown role / tag / bad bool byte.
        assert!(matches!(
            CompositeKey::decode(&[9]),
            Err(KeyDecodeError::UnknownRole(9, 0))
        ));
        let mut bad_tag = enc.clone();
        // The first value tag byte: role(1) + len(4) + "day"(3) → offset 8.
        bad_tag[8] = 200;
        assert!(matches!(
            CompositeKey::decode(&bad_tag),
            Err(KeyDecodeError::UnknownTag(200, 8))
        ));
        let bool_key = CompositeKey::new(vec![], vec![("b".into(), Value::Bool(false))]).encode();
        let mut bad_bool = bool_key.clone();
        *bad_bool.last_mut().unwrap() = 2;
        assert!(matches!(
            CompositeKey::decode(&bad_bool),
            Err(KeyDecodeError::BadBool(2, _))
        ));
        // Identifier before partition (role order) — encode never emits it; decode rejects it.
        let id_first = CompositeKey::new(vec![], vec![("id".into(), Value::Int(1))]).encode();
        let part = CompositeKey::new(vec![("p".into(), Value::Int(2))], vec![]).encode();
        let interleaved: Vec<u8> = id_first.iter().chain(part.iter()).copied().collect();
        assert!(matches!(
            CompositeKey::decode(&interleaved),
            Err(KeyDecodeError::RoleOrder(_))
        ));
    }

    #[test]
    fn values_stringify_for_indexing() {
        assert_eq!(Value::from("hi").to_index_string(), "hi");
        assert_eq!(Value::Int(7).to_index_string(), "7");
        assert_eq!(Value::Bool(true).to_index_string(), "true");
        assert_eq!(
            Value::Ts(1_782_000_000_000_000).to_index_string(),
            "1782000000000000"
        );
    }

    #[test]
    fn key_encoding_is_deterministic_and_distinguishing() {
        let k = |day: &str, id: i64| {
            CompositeKey::new(
                vec![("day".into(), Value::from(day))],
                vec![("id".into(), Value::from(id))],
            )
        };
        // Deterministic: same key → same bytes.
        assert_eq!(k("2026-06-19", 1).encode(), k("2026-06-19", 1).encode());
        // Distinguishing: any field differs → bytes differ.
        assert_ne!(k("2026-06-19", 1).encode(), k("2026-06-19", 2).encode());
        assert_ne!(k("2026-06-19", 1).encode(), k("2026-06-20", 1).encode());
        // Partition vs identifier roles don't collide: same name+value, different role.
        let a = CompositeKey::new(vec![("x".into(), Value::from(1i64))], vec![]);
        let b = CompositeKey::new(vec![], vec![("x".into(), Value::from(1i64))]);
        assert_ne!(a.encode(), b.encode());
        // Type tags distinguish "1" (string) from 1 (int).
        let s = CompositeKey::new(vec![], vec![("id".into(), Value::from("1"))]);
        let i = CompositeKey::new(vec![], vec![("id".into(), Value::from(1i64))]);
        assert_ne!(s.encode(), i.encode());
        // ... and 1 (int) from Ts(1) — same 8-byte payload, different tag.
        let t = CompositeKey::new(vec![], vec![("id".into(), Value::Ts(1))]);
        assert_ne!(i.encode(), t.encode());
    }

    /// The encoding is the routing-hash input and the Tantivy delete term, so the
    /// **exact bytes** of the existing type tags are frozen — assert them literally.
    #[test]
    fn key_encoding_golden_bytes_are_frozen_for_existing_types() {
        let key = CompositeKey::new(
            vec![("region".into(), Value::from("eu"))],
            vec![("id".into(), Value::Int(7))],
        );
        #[rustfmt::skip]
        let expected: Vec<u8> = vec![
            // role 0 · len("region") · "region" · tag 1 (Str) · len("eu") · "eu"
            0, 6, 0, 0, 0, b'r', b'e', b'g', b'i', b'o', b'n',
            1, 2, 0, 0, 0, b'e', b'u',
            // role 1 · len("id") · "id" · tag 2 (Int) · len 8 · 7 as i64 LE
            1, 2, 0, 0, 0, b'i', b'd',
            2, 8, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0,
        ];
        assert_eq!(key.encode(), expected);
    }

    /// Ts encodes under the new tag 5 as 8-byte LE micros — golden bytes so the
    /// cross-language (Java `ShardRouter`) encoding can never drift silently.
    #[test]
    fn key_encoding_golden_bytes_for_ts() {
        // ≈2026-06-21T00:00:00Z: 1_782_000_000_000_000 µs = 0x0006_54B8_34FD_6000.
        let micros: i64 = 1_782_000_000_000_000;
        let key = CompositeKey::new(vec![], vec![("day".into(), Value::Ts(micros))]);
        #[rustfmt::skip]
        let expected: Vec<u8> = vec![
            // role 1 · len("day") · "day" · tag 5 (Ts) · len 8 · micros as i64 LE
            1, 3, 0, 0, 0, b'd', b'a', b'y',
            5, 8, 0, 0, 0, 0x00, 0x60, 0xFD, 0x34, 0xB8, 0x54, 0x06, 0x00,
        ];
        assert_eq!(key.encode(), expected);
        // Round-trips deterministically like every other type.
        assert_eq!(key.encode(), key.encode());
    }

    #[test]
    fn checkpoint_round_trips_through_json() {
        let cp = SourceCheckpoint::iceberg(987654321);
        let json = serde_json::to_string(&cp).unwrap();
        assert_eq!(serde_json::from_str::<SourceCheckpoint>(&json).unwrap(), cp);
    }

    /// Sequence-numbered checkpoints round-trip, no-sequence ones keep the exact
    /// legacy JSON shape (nothing changes on disk until sequence numbers actually flow), and
    /// legacy persisted values parse.
    #[test]
    fn checkpoint_serde_is_backward_compatible() {
        let legacy: SourceCheckpoint = serde_json::from_str(r#"{"iceberg_snapshot":123}"#).unwrap();
        assert_eq!(legacy, SourceCheckpoint::iceberg(123));
        assert_eq!(
            serde_json::to_string(&legacy).unwrap(),
            r#"{"iceberg_snapshot":123}"#,
            "no-sequence checkpoints keep the legacy on-disk shape"
        );

        let ordered = SourceCheckpoint::iceberg_ordered(123, 9);
        let json = serde_json::to_string(&ordered).unwrap();
        assert_eq!(
            serde_json::from_str::<SourceCheckpoint>(&json).unwrap(),
            ordered
        );
    }

    /// Lineage order comes from the sequence number; same position is Equal even
    /// across the format seam; different positions without both sequences are incomparable —
    /// snapshot ids are random longs and must never be ordered numerically.
    #[test]
    fn checkpoint_lineage_cmp_ignores_snapshot_id_order() {
        use std::cmp::Ordering;
        let ord = SourceCheckpoint::iceberg_ordered;
        let plain = SourceCheckpoint::iceberg;

        // Numerically-backwards ids, sequence decides.
        assert_eq!(ord(900, 1).lineage_cmp(&ord(50, 2)), Some(Ordering::Less));
        assert_eq!(
            ord(50, 2).lineage_cmp(&ord(900, 1)),
            Some(Ordering::Greater)
        );
        // Same position is Equal, with or without sequences on either side.
        assert_eq!(ord(42, 7).lineage_cmp(&plain(42)), Some(Ordering::Equal));
        assert!(plain(42).same_position(&ord(42, 7)));
        // Different positions, a missing sequence ⇒ incomparable, never numeric.
        assert_eq!(plain(2).lineage_cmp(&plain(3)), None);
        assert_eq!(ord(2, 5).lineage_cmp(&plain(3)), None);
    }
}
