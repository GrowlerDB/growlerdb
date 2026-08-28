//! Hydration orchestration — the **PK lookup** path ([Flow 2]).
//!
//! Resolve composite keys through the shard's locator to `{iceberg_file, row_position}`,
//! then read the authoritative rows from Iceberg (only the located files).
//!
//! [Flow 2]: ../../../okf/system/architecture.md

use std::collections::HashSet;

use growlerdb_core::{
    CompositeKey, HydrateRequest, HydratedRow, IndexReader, LocationStrategy, Projection,
    RowLocator,
};
use growlerdb_index::Shard;
use growlerdb_source::IcebergReader;

use crate::EngineError;

/// Resolve `keys` to their `(key, locator)` pairs via the shard. A key with no
/// locator is an error.
pub fn resolve_locators(
    shard: &Shard,
    keys: &[CompositeKey],
) -> Result<Vec<(CompositeKey, RowLocator)>, EngineError> {
    let locators = IndexReader::get_by_key(shard, keys)?;
    keys.iter()
        .cloned()
        .zip(locators)
        .map(|(key, locator)| match locator {
            Some(locator) => Ok((key, locator)),
            None => Err(EngineError::MissingLocator(describe(&key))),
        })
        .collect()
}

/// Resolve `keys` into the source's hydration requests **per the shard's location
/// strategy**:
///
/// * `COORDINATES` — layered locate ([`resolve_locators`]) + live-file bitmap
///   ([`apply_live_file_bitmap`]): each key carries its `(file, position)` for the
///   pass-1 point read, `None` only when its file is flagged dead.
/// * `PREDICATE` — no location data stored; every key goes out with **no locator**,
///   straight to the source's pruned key-scan. Presence is still checked against the
///   index first, so an unindexed key is a clean `MissingLocator` (→ `NotFound`) before
///   any Iceberg connect — same contract as `COORDINATES`.
pub fn resolve_requests(
    shard: &Shard,
    keys: &[CompositeKey],
) -> Result<Vec<HydrateRequest>, EngineError> {
    match shard.location_strategy() {
        LocationStrategy::Coordinates => Ok(apply_live_file_bitmap(
            shard,
            resolve_locators(shard, keys)?,
        )),
        LocationStrategy::Predicate => keys
            .iter()
            .map(|key| match shard.contains_key(key) {
                Ok(true) => Ok(HydrateRequest::new(key.clone(), None)),
                Ok(false) => Err(EngineError::MissingLocator(describe(key))),
                Err(e) => Err(EngineError::Store(e)),
            })
            .collect(),
    }
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

/// Apply the **live-file bitmap** to resolved locators: a locator whose file the shard
/// flagged dead (rewritten away by Iceberg compaction) is known stale, so it's stripped
/// to `None` — the source's hydrate then sends the key straight to the pass-2 fallback.
/// Everything else passes through for the normal pass-1 read.
pub fn apply_live_file_bitmap(
    shard: &Shard,
    located: Vec<(CompositeKey, RowLocator)>,
) -> Vec<HydrateRequest> {
    located
        .into_iter()
        .map(|(key, locator)| {
            let live = !shard.file_is_dead(&locator.iceberg_file);
            HydrateRequest::new(key, live.then_some(locator))
        })
        .collect()
}

/// Hydrate `keys` to authoritative rows: strategy-aware request resolution
/// ([`resolve_requests`]) + a partition/file scoped Iceberg read of the projected
/// columns. Rows come back in `keys` order. Under `COORDINATES`, fallen-back locator
/// entries are **refreshed** in the store so subsequent lookups are fast again;
/// `PREDICATE` has nothing to refresh — the pruned scan is the read path itself.
///
/// **Variant fork** ([D48](../../../okf/system/decisions/d48-variant-delivery.md)/[D49]): a
/// variant-table index routes hydration through the interim Trino lane — released iceberg-rust
/// can't scan a v3 variant table. The Trino lane re-finds rows by key predicate (nothing to write
/// back), returning the variant column(s) as JSON.
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
        // Trino lane re-finds by key predicate — no per-row locator to refresh.
        return Ok(result.rows);
    }
    attach_prune_hints(&mut located, shard, source, table, keys).await;
    let result = source.hydrate(table, &located, projection).await?;
    growlerdb_telemetry::sli::duplicate_pks(result.duplicate_pks);
    if shard.location_strategy() == LocationStrategy::Coordinates {
        shard.refresh_locators(&result.refreshed)?;
    }
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

    /// A committed shard, one doc at `data/f0.parquet` row 7.
    fn committed_shard(dir: &std::path::Path) -> Shard {
        let store = LocalIndexStore::open(dir).unwrap();
        let shard = store
            .create_shard(&ShardId::single("docs"), &index())
            .unwrap();
        let mut fields = BTreeMap::new();
        fields.insert("id".to_string(), 1i64.into());
        fields.insert("body".to_string(), "hello".into());
        let doc = Document::new(key(1), fields);
        let batch = CommitBatch::from_upserts(
            vec![LocatedDoc {
                doc,
                iceberg_file: "data/f0.parquet".into(),
                row_position: 7,
            }],
            SourceCheckpoint::iceberg(1),
            "b1",
        );
        IndexWriter::write(&shard, &batch).unwrap();
        shard
    }

    #[test]
    fn resolve_locators_returns_entries_for_present_keys() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = committed_shard(tmp.path());
        let resolved = resolve_locators(&shard, &[key(1)]).unwrap();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].1.iceberg_file, "data/f0.parquet");
        assert_eq!(resolved[0].1.row_position, 7);
    }

    #[test]
    fn bitmap_strips_locators_pointing_into_dead_files() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = committed_shard(tmp.path());
        let located = resolve_locators(&shard, &[key(1)]).unwrap();
        assert!(
            apply_live_file_bitmap(&shard, located.clone())[0]
                .locator
                .is_some(),
            "live file → locator passes through to pass 1"
        );
        shard.mark_files_dead(&["data/f0.parquet".into()]).unwrap();
        let stripped = apply_live_file_bitmap(&shard, located);
        assert!(
            stripped[0].locator.is_none(),
            "dead file → known stale, straight to the fallback"
        );
        assert_eq!(stripped[0].key, key(1), "the key still hydrates via pass 2");
    }

    #[test]
    fn resolve_locators_errors_on_missing_key() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = committed_shard(tmp.path());
        // key(2) was never indexed → clear MissingLocator error, not a silent skip.
        let err = resolve_locators(&shard, &[key(1), key(2)]).unwrap_err();
        assert!(matches!(err, EngineError::MissingLocator(_)));
    }

    // ---- PREDICATE location strategy --------------------------------

    /// A committed shard on the **PREDICATE** strategy (same doc as [`committed_shard`]).
    fn committed_predicate_shard(dir: &std::path::Path) -> Shard {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("body", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nlocation_strategy: PREDICATE\nmapping: { selection: ALL }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let store = LocalIndexStore::open(dir).unwrap();
        let shard = store.create_shard(&ShardId::single("docs"), &idx).unwrap();
        let mut fields = BTreeMap::new();
        fields.insert("id".to_string(), 1i64.into());
        fields.insert("body".to_string(), "hello".into());
        let doc = Document::new(key(1), fields);
        let batch = CommitBatch::from_upserts(
            vec![LocatedDoc {
                doc,
                iceberg_file: "data/f0.parquet".into(),
                row_position: 7,
            }],
            SourceCheckpoint::iceberg(1),
            "b1",
        );
        IndexWriter::write(&shard, &batch).unwrap();
        shard
    }

    #[test]
    fn resolve_requests_on_a_predicate_shard_skips_locators_entirely() {
        // Every present key goes out with NO locator → source hydrate skips pass 1, straight
        // to the pruned key scan. No location data was ever stored to resolve anyway.
        let tmp = tempfile::tempdir().unwrap();
        let shard = committed_predicate_shard(tmp.path());
        assert_eq!(
            shard.location_strategy(),
            growlerdb_core::LocationStrategy::Predicate
        );
        let requests = resolve_requests(&shard, &[key(1)]).unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].key, key(1));
        assert!(
            requests[0].locator.is_none(),
            "predicate request carries no locator → straight to the pruned scan"
        );
    }

    #[test]
    fn resolve_requests_on_a_predicate_shard_still_rejects_missing_keys() {
        // The NotFound-before-Iceberg contract survives the strategy switch: presence
        // is checked against the index (live key term), not against location data.
        let tmp = tempfile::tempdir().unwrap();
        let shard = committed_predicate_shard(tmp.path());
        let err = resolve_requests(&shard, &[key(1), key(2)]).unwrap_err();
        assert!(matches!(err, EngineError::MissingLocator(_)));
    }

    #[test]
    fn resolve_requests_on_a_coordinates_shard_is_the_layered_locate() {
        // Default strategy: the same locator + live-file bitmap path, request-shaped.
        let tmp = tempfile::tempdir().unwrap();
        let shard = committed_shard(tmp.path());
        let requests = resolve_requests(&shard, &[key(1)]).unwrap();
        let loc = requests[0].locator.as_ref().expect("locator resolved");
        assert_eq!(loc.iceberg_file, "data/f0.parquet");
        assert_eq!(loc.row_position, 7);

        // ... and the bitmap still strips dead files to `None` (known stale).
        shard.mark_files_dead(&["data/f0.parquet".into()]).unwrap();
        let requests = resolve_requests(&shard, &[key(1)]).unwrap();
        assert!(requests[0].locator.is_none(), "dead file → pass-2 fallback");
    }
}
