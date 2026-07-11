//! `growlerdb-proto` — the `growlerdb.v1` gRPC wire schema ([Design 08]): the generated
//! message and service types, the bridge to `growlerdb-core` vocabulary, the error
//! convention, and a minimal `System` (health/version) service that proves the
//! toolchain.
//!
//! The proto is canonical; breaking changes bump the package to `growlerdb.v2`.
//! Real services (Write, Engine API) add their own protos and grow this surface per service.
//!
//! [Design 08]: ../../../design/08-schemas.md

use tonic::{Request, Response, Status};

/// Generated `growlerdb.v1` types and services (from `proto/growlerdb/v1/*.proto`).
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/growlerdb.v1.rs"));
}

pub mod service_token;

pub use v1::admin_server::{Admin, AdminServer};
pub use v1::control_plane_client::ControlPlaneClient;
pub use v1::control_plane_server::{ControlPlane, ControlPlaneServer};
pub use v1::lookup_server::{Lookup, LookupServer};
pub use v1::search_server::{Search, SearchServer};
pub use v1::suggest_server::{Suggest, SuggestServer};
pub use v1::system_server::{System, SystemServer};
pub use v1::write_client::WriteClient;
pub use v1::write_server::{Write, WriteServer};

// ---- growlerdb-core ↔ wire bridge -----------------------------------------------

/// A wire value carried no payload where one was required.
#[derive(Debug, thiserror::Error)]
#[error("missing or empty `{0}` in wire message")]
pub struct MissingField(pub &'static str);

impl From<growlerdb_core::Value> for v1::Value {
    fn from(value: growlerdb_core::Value) -> Self {
        use v1::value::Kind;
        let kind = match value {
            growlerdb_core::Value::Str(s) => Kind::Str(s),
            growlerdb_core::Value::Int(i) => Kind::Int(i),
            growlerdb_core::Value::Float(f) => Kind::Float(f),
            growlerdb_core::Value::Bool(b) => Kind::Bool(b),
            growlerdb_core::Value::Ts(t) => Kind::TsMicros(t),
        };
        v1::Value { kind: Some(kind) }
    }
}

impl TryFrom<v1::Value> for growlerdb_core::Value {
    type Error = MissingField;
    fn try_from(value: v1::Value) -> Result<Self, MissingField> {
        use v1::value::Kind;
        match value.kind.ok_or(MissingField("Value.kind"))? {
            Kind::Str(s) => Ok(growlerdb_core::Value::Str(s)),
            Kind::Int(i) => Ok(growlerdb_core::Value::Int(i)),
            Kind::Float(f) => Ok(growlerdb_core::Value::Float(f)),
            Kind::Bool(b) => Ok(growlerdb_core::Value::Bool(b)),
            Kind::TsMicros(t) => Ok(growlerdb_core::Value::Ts(t)),
        }
    }
}

impl From<&growlerdb_core::CompositeKey> for v1::Coordinates {
    fn from(key: &growlerdb_core::CompositeKey) -> Self {
        let fields = |pairs: &[(String, growlerdb_core::Value)]| {
            pairs
                .iter()
                .map(|(name, value)| v1::Field {
                    name: name.clone(),
                    value: Some(value.clone().into()),
                })
                .collect()
        };
        v1::Coordinates {
            partition: fields(&key.partition),
            identifier: fields(&key.identifier),
        }
    }
}

impl TryFrom<v1::Coordinates> for growlerdb_core::CompositeKey {
    type Error = MissingField;
    fn try_from(coords: v1::Coordinates) -> Result<Self, MissingField> {
        let fields = |fields: Vec<v1::Field>| {
            fields
                .into_iter()
                .map(|f| {
                    Ok((
                        f.name,
                        f.value.ok_or(MissingField("Field.value"))?.try_into()?,
                    ))
                })
                .collect::<Result<Vec<_>, MissingField>>()
        };
        Ok(growlerdb_core::CompositeKey::new(
            fields(coords.partition)?,
            fields(coords.identifier)?,
        ))
    }
}

impl From<&growlerdb_core::SortValue> for v1::SortValue {
    fn from(v: &growlerdb_core::SortValue) -> Self {
        use v1::sort_value::Kind;
        let kind = match v {
            growlerdb_core::SortValue::Num(n) => Kind::Num(*n),
            growlerdb_core::SortValue::Str(s) => Kind::Str(s.clone()),
            growlerdb_core::SortValue::Missing => Kind::Missing(true),
        };
        v1::SortValue { kind: Some(kind) }
    }
}

impl TryFrom<v1::SortValue> for growlerdb_core::SortValue {
    type Error = MissingField;
    fn try_from(v: v1::SortValue) -> Result<Self, MissingField> {
        use v1::sort_value::Kind;
        Ok(match v.kind.ok_or(MissingField("SortValue.kind"))? {
            Kind::Num(n) => growlerdb_core::SortValue::Num(n),
            Kind::Str(s) => growlerdb_core::SortValue::Str(s),
            // Any `missing` variant (regardless of the bool) decodes to Missing.
            Kind::Missing(_) => growlerdb_core::SortValue::Missing,
        })
    }
}

impl From<growlerdb_core::SourceCheckpoint> for v1::SourceCheckpoint {
    fn from(cp: growlerdb_core::SourceCheckpoint) -> Self {
        v1::SourceCheckpoint {
            kind: Some(v1::source_checkpoint::Kind::IcebergSnapshot(
                cp.snapshot_id(),
            )),
            // 0 = unknown on the wire (proto3 default); real Iceberg sequence numbers
            // start at 1 (v1 tables report 0, i.e. unknown — the same fallback).
            iceberg_sequence_number: cp.sequence_number().unwrap_or(0),
        }
    }
}

impl TryFrom<v1::SourceCheckpoint> for growlerdb_core::SourceCheckpoint {
    type Error = MissingField;
    fn try_from(cp: v1::SourceCheckpoint) -> Result<Self, MissingField> {
        match cp.kind.ok_or(MissingField("SourceCheckpoint.kind"))? {
            v1::source_checkpoint::Kind::IcebergSnapshot(id) => {
                Ok(match cp.iceberg_sequence_number {
                    seq if seq > 0 => growlerdb_core::SourceCheckpoint::iceberg_ordered(id, seq),
                    _ => growlerdb_core::SourceCheckpoint::iceberg(id),
                })
            }
        }
    }
}

// ---- Write path bridge (DocBatch / DocOp / Document) -----------------------

impl From<growlerdb_core::Document> for v1::Document {
    fn from(doc: growlerdb_core::Document) -> Self {
        v1::Document {
            key: Some((&doc.key).into()),
            fields: doc
                .fields
                .into_iter()
                .map(|(name, value)| (name, value.into()))
                .collect(),
        }
    }
}

impl TryFrom<v1::Document> for growlerdb_core::Document {
    type Error = MissingField;
    fn try_from(doc: v1::Document) -> Result<Self, MissingField> {
        let key = doc.key.ok_or(MissingField("Document.key"))?.try_into()?;
        let fields = doc
            .fields
            .into_iter()
            .map(|(name, value)| Ok((name, value.try_into()?)))
            .collect::<Result<_, MissingField>>()?;
        Ok(growlerdb_core::Document::new(key, fields))
    }
}

impl From<growlerdb_core::LocatedDoc> for v1::LocatedDoc {
    fn from(d: growlerdb_core::LocatedDoc) -> Self {
        v1::LocatedDoc {
            doc: Some(d.doc.into()),
            iceberg_file: d.iceberg_file,
            row_position: d.row_position,
        }
    }
}

impl TryFrom<v1::LocatedDoc> for growlerdb_core::LocatedDoc {
    type Error = MissingField;
    fn try_from(d: v1::LocatedDoc) -> Result<Self, MissingField> {
        Ok(growlerdb_core::LocatedDoc {
            doc: d.doc.ok_or(MissingField("LocatedDoc.doc"))?.try_into()?,
            iceberg_file: d.iceberg_file,
            row_position: d.row_position,
        })
    }
}

impl From<growlerdb_core::DocOp> for v1::DocOp {
    fn from(op: growlerdb_core::DocOp) -> Self {
        let op = match op {
            growlerdb_core::DocOp::Upsert(d) => v1::doc_op::Op::Upsert(d.into()),
            growlerdb_core::DocOp::Delete(k) => v1::doc_op::Op::Delete((&k).into()),
        };
        v1::DocOp { op: Some(op) }
    }
}

impl TryFrom<v1::DocOp> for growlerdb_core::DocOp {
    type Error = MissingField;
    fn try_from(op: v1::DocOp) -> Result<Self, MissingField> {
        match op.op.ok_or(MissingField("DocOp.op"))? {
            v1::doc_op::Op::Upsert(d) => Ok(growlerdb_core::DocOp::Upsert(d.try_into()?)),
            v1::doc_op::Op::Delete(k) => Ok(growlerdb_core::DocOp::Delete(k.try_into()?)),
        }
    }
}

impl From<growlerdb_core::CommitBatch> for v1::DocBatch {
    fn from(batch: growlerdb_core::CommitBatch) -> Self {
        v1::DocBatch {
            ops: batch.ops.into_iter().map(Into::into).collect(),
            checkpoint: Some(batch.checkpoint.into()),
            batch_id: batch.batch_id,
            from_checkpoint: batch.from_checkpoint.map(Into::into),
            safe_checkpoint: batch.safe_checkpoint.map(Into::into),
        }
    }
}

impl TryFrom<v1::DocBatch> for growlerdb_core::CommitBatch {
    type Error = MissingField;
    fn try_from(batch: v1::DocBatch) -> Result<Self, MissingField> {
        let ops = batch
            .ops
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, MissingField>>()?;
        let checkpoint = batch
            .checkpoint
            .ok_or(MissingField("DocBatch.checkpoint"))?
            .try_into()?;
        // `from_checkpoint` is optional on the wire (absent = from the start of the changelog); the
        // Node's continuity guard only engages when it is present.
        let from_checkpoint = batch.from_checkpoint.map(TryInto::try_into).transpose()?;
        // `safe_checkpoint` is the connector's resume floor; absent = no floor yet, so the write path
        // prunes nothing from the idempotency store.
        let safe_checkpoint = batch.safe_checkpoint.map(TryInto::try_into).transpose()?;
        Ok(
            growlerdb_core::CommitBatch::new(ops, checkpoint, batch.batch_id)
                .with_from_checkpoint(from_checkpoint)
                .with_safe_checkpoint(safe_checkpoint),
        )
    }
}

// ---- Error convention ------------------------------------------------------

impl v1::Error {
    /// A structured error with a code + message.
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        v1::Error {
            code: code.into(),
            message: message.into(),
            details: Vec::new(),
        }
    }
}

/// The GrowlerDB wire error convention: a gRPC [`Status`] whose message is the
/// structured [`v1::Error`]'s, with the encoded `Error` attached as status
/// details (so a client gets both the gRPC code and the structured payload).
pub fn to_status(grpc_code: tonic::Code, error: v1::Error) -> Status {
    use prost::Message;
    Status::with_details(
        grpc_code,
        error.message.clone(),
        error.encode_to_vec().into(),
    )
}

/// Decode the structured [`v1::Error`] from a status's details, if present.
pub fn error_details(status: &Status) -> Option<v1::Error> {
    use prost::Message;
    if status.details().is_empty() {
        return None;
    }
    v1::Error::decode(status.details()).ok()
}

// ---- System service --------------------------------------------------------

/// Minimal `System` service: liveness + version. Mountable on any GrowlerDB binary's
/// gRPC server; proves the wire toolchain end to end.
#[derive(Debug, Clone)]
pub struct SystemService {
    version: String,
}

impl SystemService {
    /// A `System` service reporting `version`.
    pub fn new(version: impl Into<String>) -> Self {
        Self {
            version: version.into(),
        }
    }
}

#[tonic::async_trait]
impl System for SystemService {
    async fn health(
        &self,
        _req: Request<v1::HealthRequest>,
    ) -> Result<Response<v1::HealthResponse>, Status> {
        Ok(Response::new(v1::HealthResponse {
            status: v1::health_response::Status::Serving as i32,
        }))
    }

    async fn version(
        &self,
        _req: Request<v1::VersionRequest>,
    ) -> Result<Response<v1::VersionResponse>, Status> {
        Ok(Response::new(v1::VersionResponse {
            version: self.version.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_bridges_round_trip() {
        for v in [
            growlerdb_core::Value::Str("hi".into()),
            growlerdb_core::Value::Int(7),
            growlerdb_core::Value::Float(1.5),
            growlerdb_core::Value::Bool(true),
            growlerdb_core::Value::Ts(1_782_000_000_000_000),
        ] {
            let wire: v1::Value = v.clone().into();
            assert_eq!(growlerdb_core::Value::try_from(wire).unwrap(), v);
        }
    }

    #[test]
    fn coordinates_bridge_round_trip() {
        let key = growlerdb_core::CompositeKey::new(
            vec![("day".into(), growlerdb_core::Value::from("2026-06-20"))],
            vec![("id".into(), growlerdb_core::Value::from(42i64))],
        );
        let wire: v1::Coordinates = (&key).into();
        assert_eq!(growlerdb_core::CompositeKey::try_from(wire).unwrap(), key);
    }

    #[test]
    fn checkpoint_bridge_round_trip() {
        let cp = growlerdb_core::SourceCheckpoint::iceberg(99);
        let wire: v1::SourceCheckpoint = cp.clone().into();
        assert_eq!(wire.iceberg_sequence_number, 0, "no sequence ⇒ 0 (unknown)");
        assert_eq!(
            growlerdb_core::SourceCheckpoint::try_from(wire).unwrap(),
            cp
        );

        // The lineage sequence number rides the same message.
        let ordered = growlerdb_core::SourceCheckpoint::iceberg_ordered(99, 7);
        let wire: v1::SourceCheckpoint = ordered.clone().into();
        assert_eq!(wire.iceberg_sequence_number, 7);
        assert_eq!(
            growlerdb_core::SourceCheckpoint::try_from(wire).unwrap(),
            ordered
        );
    }

    #[test]
    fn error_round_trips_through_status_details() {
        let err = v1::Error::new("INVALID_ARGUMENT", "bad query");
        let status = to_status(tonic::Code::InvalidArgument, err.clone());
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(status.message(), "bad query");
        let decoded = error_details(&status).expect("structured error");
        assert_eq!(decoded.code, "INVALID_ARGUMENT");
    }
}
