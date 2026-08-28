//! Hydration orchestration — the **PK lookup** path ([Flow 2]).
//!
//! Store-less: the index keeps no location data. Each key is checked for presence, then the source
//! re-finds its authoritative row by a key-equality scan pruned to the matching Iceberg row groups
//! by the row's own stored sort-key value ([`attach_prune_hints`]).
//!
//! [Flow 2]: ../../../okf/system/architecture.md

use std::collections::HashSet;

use growlerdb_core::{CompositeKey, HydrateRequest, HydratedRow, Projection};
use growlerdb_index::Shard;
use growlerdb_source::IcebergReader;

use crate::EngineError;

/// Resolve `keys` into hydration requests. Presence is checked against the index (a live key term),
/// so an unindexed key is a clean `KeyNotFound` (→ `NotFound`) before any Iceberg connect; a
/// present key goes out to the source's pruned key-scan (prune hints attached later).
pub fn resolve_requests(
    shard: &Shard,
    keys: &[CompositeKey],
) -> Result<Vec<HydrateRequest>, EngineError> {
    keys.iter()
        .map(|key| match shard.contains_key(key) {
            Ok(true) => Ok(HydrateRequest::new(key.clone())),
            Ok(false) => Err(EngineError::KeyNotFound(describe(key))),
            Err(e) => Err(EngineError::Store(e)),
        })
        .collect()
}

/// Attach **sort-key prune hints** to `requests` (in place) — the values the source's pass-2 key
/// predicate AND-s on so a **sorted** source table prunes files by manifest min/max on the sort key
/// (the heal for an unpartitioned, hash-routed random identifier whose per-file min/max spans the
/// whole space, TASK-339). The hint fields are the table's identity sort columns the shard also
/// stores fast (excluding the key's own fields, already in the predicate); their values are the
/// row's own fast-field values, aligned 1:1 with `keys`.
///
/// **Best-effort**: any failure (a catalog metadata read, a shard fast-field read) just leaves the
/// hints empty — the predicate is a pure prune, correctness rests on the exact key re-verify. Costs
/// one metadata load plus a fast-field read per key; skipped entirely on an unsorted table.
pub(crate) async fn attach_prune_hints(
    requests: &mut [HydrateRequest],
    shard: &Shard,
    source: &IcebergReader,
    table: &str,
    keys: &[CompositeKey],
) {
    let Some(first) = keys.first() else {
        return;
    };
    let Ok(sort_names) = source.sort_field_names(table).await else {
        return;
    };
    if sort_names.is_empty() {
        return;
    }
    let fast: HashSet<String> = shard.sort_fields().into_iter().collect();
    let key_fields: HashSet<&str> = first
        .partition
        .iter()
        .chain(first.identifier.iter())
        .map(|(n, _)| n.as_str())
        .collect();
    let hint_fields: Vec<String> = sort_names
        .into_iter()
        .filter(|n| fast.contains(n) && !key_fields.contains(n.as_str()))
        .collect();
    if hint_fields.is_empty() {
        return;
    }
    let Ok(values) = shard.prune_values(keys, &hint_fields) else {
        return;
    };
    for (req, v) in requests.iter_mut().zip(values) {
        req.prune = v;
    }
}

/// Hydrate `keys` to authoritative rows: presence-checked request resolution
/// ([`resolve_requests`]) + a partition/row-group-scoped Iceberg key scan of the projected
/// columns, pruned by the rows' own sort-key values ([`attach_prune_hints`]). Rows come back in
/// `keys` order. Store-less — there is nothing to write back.
///
/// **Variant fork** ([D48](../../../okf/system/decisions/d48-variant-delivery.md)/[D49]): a
/// variant-table index routes hydration through the interim Trino lane — released iceberg-rust
/// can't scan a v3 variant table. The Trino lane re-finds rows by key predicate, returning the
/// variant column(s) as JSON.
///
/// [D49]: ../../../okf/system/decisions/d49-variant-iceberg-rust-routing.md
pub async fn get_by_key(
    shard: &Shard,
    source: &IcebergReader,
    index: &growlerdb_core::ResolvedIndex,
    table: &str,
    keys: &[CompositeKey],
    projection: &Projection,
) -> Result<Vec<HydratedRow>, EngineError> {
    let mut located = resolve_requests(shard, keys)?;
    if index.has_variant_field() {
        // Trino re-finds by key predicate — no manifest pruning, so no sort-key hints needed.
        let result = growlerdb_source::shared_hydrator()
            .hydrate(index, &located, projection)
            .await?;
        growlerdb_telemetry::sli::duplicate_pks(result.duplicate_pks);
        return Ok(result.rows);
    }
    attach_prune_hints(&mut located, shard, source, table, keys).await;
    let result = source.hydrate(table, &located, projection).await?;
    growlerdb_telemetry::sli::duplicate_pks(result.duplicate_pks);
    Ok(result.rows)
}

/// A compact, human-readable rendering of a key for error messages.
fn describe(key: &CompositeKey) -> String {
    let part = |fields: &[(String, growlerdb_core::Value)]| {
        fields
            .iter()
            .map(|(n, v)| format!("{n}={}", v.to_index_string()))
            .collect::<Vec<_>>()
            .join(",")
    };
    format!("[{}|{}]", part(&key.partition), part(&key.identifier))
}

#[cfg(test)]
mod tests {
    use super::*;
    use growlerdb_core::{
        CommitBatch, Document, IndexDefinition, IndexWriter, LocatedDoc, ResolvedIndex,
        SourceCheckpoint, SourceField, SourceSchema, SourceType,
    };
    use growlerdb_index::{LocalIndexStore, ShardId};
    use std::collections::BTreeMap;

    fn index() -> ResolvedIndex {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("body", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: ALL }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap()
    }

    fn key(id: i64) -> CompositeKey {
        CompositeKey::new(vec![], vec![("id".into(), id.into())])
    }

    /// A committed shard, one indexed doc for `key(1)`.
    fn committed_shard(dir: &std::path::Path) -> Shard {
        let store = LocalIndexStore::open(dir).unwrap();
        let shard = store
            .create_shard(&ShardId::single("docs"), &index())
            .unwrap();
        let mut fields = BTreeMap::new();
        fields.insert("id".to_string(), 1i64.into());
        fields.insert("body".to_string(), "hello".into());
        let doc = Document::new(key(1), fields);
        let batch =
            CommitBatch::from_upserts(vec![LocatedDoc { doc }], SourceCheckpoint::iceberg(1), "b1");
        IndexWriter::write(&shard, &batch).unwrap();
        shard
    }

    #[test]
    fn resolve_requests_builds_a_request_per_present_key() {
        // Store-less: presence is checked against the index (a live key term); a present key yields
        // a request straight to the pruned key scan — no location data was ever stored.
        let tmp = tempfile::tempdir().unwrap();
        let shard = committed_shard(tmp.path());
        let requests = resolve_requests(&shard, &[key(1)]).unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].key, key(1));
    }

    #[test]
    fn resolve_requests_rejects_missing_keys() {
        // The NotFound-before-Iceberg contract: an unindexed key is a clean KeyNotFound error,
        // not a silent skip — checked against the index, before any Iceberg connect.
        let tmp = tempfile::tempdir().unwrap();
        let shard = committed_shard(tmp.path());
        let err = resolve_requests(&shard, &[key(1), key(2)]).unwrap_err();
        assert!(matches!(err, EngineError::KeyNotFound(_)));
    }
}
