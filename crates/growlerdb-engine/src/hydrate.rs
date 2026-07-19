//! Hydration orchestration — the **PK lookup** path ([Flow 2]).
//!
//! Given search coordinates (composite keys), resolve each through the shard's
//! [locator](growlerdb_core::RowLocator) to `{iceberg_file, row_position}`, then read
//! the authoritative rows from Iceberg (reading only the located files). This ties
//! the [`IndexReader`] (locator) and the [`IcebergReader`] (rows) together; the
//! engine façade and CLI drive it.
//!
//! [Flow 2]: ../../../okf/system/architecture.md

use growlerdb_core::{
    CompositeKey, HydratedRow, IndexReader, LocationStrategy, Projection, RowLocator,
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
/// * `COORDINATES` — the layered locate ([`resolve_locators`]) + the live-file bitmap
///   ([`apply_live_file_bitmap`]): each key carries its `(file, position)` for the
///   pass-1 point read, `None` only when its file is flagged dead.
/// * `PREDICATE` — the store holds no location data; every key goes out with **no
///   locator**, which sends it straight to the source's pruned key-scan (the pass-2
///   machinery *is* this strategy's primary path). Key **presence** is still checked
///   against the index first, so an unindexed key is a clean `MissingLocator`
///   (→ `NotFound`) before any Iceberg connect — same contract as `COORDINATES`.
pub fn resolve_requests(
    shard: &Shard,
    keys: &[CompositeKey],
) -> Result<Vec<(CompositeKey, Option<RowLocator>)>, EngineError> {
    match shard.location_strategy() {
        LocationStrategy::Coordinates => Ok(apply_live_file_bitmap(
            shard,
            resolve_locators(shard, keys)?,
        )),
        LocationStrategy::Predicate => keys
            .iter()
            .map(|key| match shard.contains_key(key) {
                Ok(true) => Ok((key.clone(), None)),
                Ok(false) => Err(EngineError::MissingLocator(describe(key))),
                Err(e) => Err(EngineError::Store(e)),
            })
            .collect(),
    }
}

/// Apply the **live-file bitmap** to resolved locators: a locator
/// whose file the shard has flagged dead (rewritten away by Iceberg compaction) is
/// **known stale** — its point read is doomed — so it's stripped to `None` and the
/// source's hydrate sends the key straight to the pass-2 fallback (whose result then
/// refreshes the slot). Everything else passes through for the normal pass-1 read.
pub fn apply_live_file_bitmap(
    shard: &Shard,
    located: Vec<(CompositeKey, RowLocator)>,
) -> Vec<(CompositeKey, Option<RowLocator>)> {
    located
        .into_iter()
        .map(|(key, locator)| {
            let live = !shard.file_is_dead(&locator.iceberg_file);
            (key, live.then_some(locator))
        })
        .collect()
}

/// Hydrate `keys` to authoritative rows: strategy-aware request resolution
/// ([`resolve_requests`]) + a partition/file scoped Iceberg read of the projected
/// columns. Rows come back in `keys` order. Under `COORDINATES`, locator entries that
/// fell back (Iceberg rewrote their file) are **refreshed** in the store so subsequent
/// lookups are fast again; under `PREDICATE` there is nothing to refresh — the pruned
/// scan is the read path itself, not a fallback.
pub async fn get_by_key(
    shard: &Shard,
    source: &IcebergReader,
    table: &str,
    keys: &[CompositeKey],
    projection: &Projection,
) -> Result<Vec<HydratedRow>, EngineError> {
    let located = resolve_requests(shard, keys)?;
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
        let shard = committed_shard(tmp.path()); // one doc located in data/f0.parquet
        let located = resolve_locators(&shard, &[key(1)]).unwrap();
        assert!(
            apply_live_file_bitmap(&shard, located.clone())[0]
                .1
                .is_some(),
            "live file → locator passes through to pass 1"
        );
        shard.mark_files_dead(&["data/f0.parquet".into()]).unwrap();
        let stripped = apply_live_file_bitmap(&shard, located);
        assert!(
            stripped[0].1.is_none(),
            "dead file → known stale, straight to the fallback"
        );
        assert_eq!(stripped[0].0, key(1), "the key still hydrates via pass 2");
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
        // Every present key goes out with NO locator — the source's hydrate then skips
        // pass 1 and sends it straight to the pruned key scan (the strategy's primary
        // path). No location data was ever stored to resolve anyway.
        let tmp = tempfile::tempdir().unwrap();
        let shard = committed_predicate_shard(tmp.path());
        assert_eq!(
            shard.location_strategy(),
            growlerdb_core::LocationStrategy::Predicate
        );
        let requests = resolve_requests(&shard, &[key(1)]).unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0, key(1));
        assert!(
            requests[0].1.is_none(),
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
        // Zero behavior change for the default strategy: the same locator + live-file
        // bitmap path as before, request-shaped.
        let tmp = tempfile::tempdir().unwrap();
        let shard = committed_shard(tmp.path());
        let requests = resolve_requests(&shard, &[key(1)]).unwrap();
        let loc = requests[0].1.as_ref().expect("locator resolved");
        assert_eq!(loc.iceberg_file, "data/f0.parquet");
        assert_eq!(loc.row_position, 7);

        // ... and the bitmap still strips dead files to `None` (known stale).
        shard.mark_files_dead(&["data/f0.parquet".into()]).unwrap();
        let requests = resolve_requests(&shard, &[key(1)]).unwrap();
        assert!(requests[0].1.is_none(), "dead file → pass-2 fallback");
    }
}
