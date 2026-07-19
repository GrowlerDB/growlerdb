//! **Shard routing** ([Service architecture]): map a document [`CompositeKey`] to a shard
//! ordinal. The same router places writes (the connector picks the owning shard) and routes
//! key lookups (the Gateway sends a key only to its shard), so both agree on placement.
//!
//! Default is **hash** routing (uniform spread over the full key). An index keyed by a
//! partition field uses **partition** routing, which co-locates a whole partition on one
//! shard — so partition-scoped queries hit fewer shards. The hash is FNV-1a over the key's
//! stable byte encoding: deterministic across processes and releases, no dependency.
//!
//! Two **placement** modes:
//! * **Legacy** — `shard = fnv1a(key) % shards`. The direct mapping; resharding
//!   re-routes (almost) every key, so it's reindex-only. Indexes without a bucket
//!   layer use this, and it stays the default so existing data is never misplaced.
//! * **Bucketed** — `bucket = fnv1a(key) % `[`NUM_BUCKETS`]`; shard = `bucket→shard map. A fixed
//!   virtual-bucket layer (consistent hashing): growing/shrinking shards moves whole **buckets**
//!   ([`BucketMap::reassign`]) — bounded data movement (~1/N), online — instead of re-routing
//!   every key. A [balanced](BucketMap::balanced) map over a shard count that divides
//!   `NUM_BUCKETS` reproduces legacy placement exactly, so the two agree on power-of-two counts.
//!
//! [Service architecture]: ../../../okf/system/architecture.md

use std::sync::Arc;

use crate::api::CommitBatch;
use crate::doc::CompositeKey;

/// Number of **virtual buckets**: keys hash into `0..NUM_BUCKETS`, and a
/// [`BucketMap`] assigns each bucket to a shard. Fixed for the cluster's life. A power of two,
/// so a [balanced](BucketMap::balanced) map over any power-of-two shard count reproduces legacy
/// `fnv % shards` placement exactly (`(fnv % NUM_BUCKETS) % shards == fnv % shards` when
/// `shards | NUM_BUCKETS`). 1024 ≫ realistic shard counts, so buckets-per-shard stays even.
pub const NUM_BUCKETS: u32 = 1024;

/// How a [`ShardRouter`] maps keys to shards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingStrategy {
    /// Hash the full key — a uniform spread (the default for an unpartitioned index).
    Hash,
    /// Hash the key's **partition** fields, so a partition's documents share a shard. Falls
    /// back to the full key when a key has no partition fields.
    Partition,
}

/// Physical placement of routed keys — legacy direct modulo, or the virtual-bucket map.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Placement {
    /// `shard = fnv1a(key) % shards` (clamped ≥ 1). The direct mapping; resharding is a rebuild.
    Legacy { shards: u32 },
    /// `shard = map[fnv1a(key) % NUM_BUCKETS]`. Resharding moves whole buckets.
    Bucketed(Arc<BucketMap>),
}

/// Routes a [`CompositeKey`] to a shard ordinal in `0..shards`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRouter {
    strategy: RoutingStrategy,
    placement: Placement,
}

impl ShardRouter {
    /// A **legacy** router over `shards` shards (clamped to at least 1) with the given strategy.
    pub fn new(shards: u32, strategy: RoutingStrategy) -> Self {
        Self {
            strategy,
            placement: Placement::Legacy {
                shards: shards.max(1),
            },
        }
    }

    /// Legacy hash routing over `shards` shards — the default for an unpartitioned index.
    pub fn hashed(shards: u32) -> Self {
        Self::new(shards, RoutingStrategy::Hash)
    }

    /// Legacy partition routing over `shards` shards — co-locate a partition on one shard.
    pub fn partitioned(shards: u32) -> Self {
        Self::new(shards, RoutingStrategy::Partition)
    }

    /// A **bucketed** router: keys hash to a bucket, `map` assigns buckets to shards.
    /// Resharding moves whole buckets ([`BucketMap::reassign`]) instead of re-routing every key.
    pub fn bucketed(strategy: RoutingStrategy, map: BucketMap) -> Self {
        Self {
            strategy,
            placement: Placement::Bucketed(Arc::new(map)),
        }
    }

    /// Build the router an index's **registry routing config** describes — the single
    /// interpretation both the Gateway (reads) and the connector (writes) use, so they can't drift:
    /// an **empty** `bucket_owners` ⇒ legacy `fnv % shard_count` (only an index no node has
    /// announced yet — registration adopts a map); a non-empty one ⇒ bucketed over that map.
    ///
    /// When a map is present it is the **sole** source of truth for the routed shard count —
    /// `shard_count` (the assigned/registered count) is deliberately ignored: during an online
    /// grow the new build targets register *before* the cutover, so the assigned count runs ahead
    /// of the routed count, and sizing (or arity-checking) the router by it would either misroute
    /// keys to still-empty shards or refuse to build routing at all for the whole rebuild window.
    /// Errors only if a stored map is malformed.
    pub fn from_registry(
        strategy: RoutingStrategy,
        bucket_owners: &[u32],
        shard_count: u32,
    ) -> Result<Self, String> {
        if bucket_owners.is_empty() {
            return Ok(Self::new(shard_count, strategy));
        }
        let map = BucketMap::from_owners(bucket_owners.to_vec())?;
        Ok(Self::bucketed(strategy, map))
    }

    /// The shard count this router spreads over.
    pub fn shards(&self) -> u32 {
        match &self.placement {
            Placement::Legacy { shards } => *shards,
            Placement::Bucketed(map) => map.shards(),
        }
    }

    /// The [`BucketMap`] when this is a [bucketed](Self::bucketed) router, else `None` (legacy).
    pub fn bucket_map(&self) -> Option<&BucketMap> {
        match &self.placement {
            Placement::Bucketed(map) => Some(map),
            Placement::Legacy { .. } => None,
        }
    }

    /// The **bucket** (`0..`[`NUM_BUCKETS`]) a key hashes to under this router's strategy —
    /// independent of placement. Drives bucket-level migration and skew diagnostics.
    pub fn bucket(&self, key: &CompositeKey) -> u32 {
        (fnv1a(&self.strategy_bytes(key)) % u64::from(NUM_BUCKETS)) as u32
    }

    /// The bytes hashed for routing under this router's strategy.
    fn strategy_bytes(&self, key: &CompositeKey) -> Vec<u8> {
        match self.strategy {
            RoutingStrategy::Hash => key.encode(),
            RoutingStrategy::Partition if !key.partition.is_empty() => {
                CompositeKey::new(key.partition.clone(), Vec::new()).encode()
            }
            // Partition routing on a key with no partition fields ⇒ behave like hash.
            RoutingStrategy::Partition => key.encode(),
        }
    }

    /// The shard ordinal (`0..shards`) that owns `key`.
    pub fn route(&self, key: &CompositeKey) -> u32 {
        match &self.placement {
            Placement::Legacy { shards } => {
                if *shards <= 1 {
                    return 0;
                }
                (fnv1a(&self.strategy_bytes(key)) % u64::from(*shards)) as u32
            }
            Placement::Bucketed(map) => map.owner(self.bucket(key)),
        }
    }

    /// Whether shard `ordinal` owns `key` under this router — the filter a node applies when it
    /// **rebuilds its shard for a reshard**: re-derive from source but keep only the docs
    /// this shard owns under the new map. Equivalent to `route(key) == ordinal`.
    pub fn owns(&self, key: &CompositeKey, ordinal: u32) -> bool {
        self.route(key) == ordinal
    }

    /// Place a [`CommitBatch`] across shards: [`route`](Self::route) every op by its key and
    /// group into one sub-batch per shard ordinal `0..shards`, **preserving op order within
    /// each shard**. Because every op for a given key routes to the same shard, per-key order
    /// (e.g. upsert-then-delete) is preserved too.
    ///
    /// Returns exactly [`shards`](Self::shards) sub-batches, indexed by ordinal. Each carries
    /// the **same `checkpoint`** (so all shards advance to the same source position) and a
    /// per-shard `batch_id` (`{batch_id}#s{ordinal}`) so idempotent retries stay shard-unique.
    /// A shard with no ops in this batch gets an **empty** sub-batch — keep it to advance that
    /// shard's checkpoint, or skip empties (`!b.ops.is_empty()`) if checkpoints advance another
    /// way.
    pub fn partition_batch(&self, batch: &CommitBatch) -> Vec<CommitBatch> {
        let mut per_shard: Vec<Vec<crate::api::DocOp>> = vec![Vec::new(); self.shards() as usize];
        for op in &batch.ops {
            let shard = self.route(op.key()) as usize;
            per_shard[shard].push(op.clone());
        }
        per_shard
            .into_iter()
            .enumerate()
            .map(|(ordinal, ops)| {
                // Carry the `from` checkpoint onto every sub-batch: each shard's continuity guard
                // needs it, and all shards resume from the same source position. Carry the
                // safe-checkpoint resume floor too: it is the same across shards, and a sub-batch
                // without it would prune nothing on its shard.
                CommitBatch::new(
                    ops,
                    batch.checkpoint.clone(),
                    format!("{}#s{ordinal}", batch.batch_id),
                )
                .with_from_checkpoint(batch.from_checkpoint.clone())
                .with_safe_checkpoint(batch.safe_checkpoint.clone())
            })
            .collect()
    }
}

/// FNV-1a 64-bit — a small, stable, dependency-free hash.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// The **bucket → shard** assignment: the registry's elastic placement table. Each of
/// the [`NUM_BUCKETS`] buckets names its owning shard ordinal; growing/shrinking shards
/// ([`reassign`](Self::reassign)) moves whole buckets — a bounded set (~1/N) — instead of
/// re-routing every key. The default ([`balanced`](Self::balanced)) is round-robin `b % shards`,
/// which equals legacy placement when `shards` divides `NUM_BUCKETS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketMap {
    /// `owners[b]` = the shard ordinal that owns bucket `b`. Length is always [`NUM_BUCKETS`].
    owners: Vec<u32>,
}

/// The result of a [reassignment](BucketMap::reassign): the new map plus the buckets that moved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reassignment {
    /// The balanced map over the new shard count.
    pub map: BucketMap,
    /// `(bucket, from_shard, to_shard)` for each bucket whose owner changed — the migration work.
    pub moved: Vec<(u32, u32, u32)>,
}

impl BucketMap {
    /// The default **balanced** map over `shards` shards (clamped ≥ 1): round-robin `b % shards`.
    /// Equals legacy `fnv % shards` placement whenever `shards` divides [`NUM_BUCKETS`] (all
    /// power-of-two counts), so a legacy index can adopt buckets without moving any key.
    pub fn balanced(shards: u32) -> Self {
        let shards = shards.max(1);
        Self {
            owners: (0..NUM_BUCKETS).map(|b| b % shards).collect(),
        }
    }

    /// Build a map from an explicit `owners` table (e.g. loaded from the registry). Errors unless
    /// it has exactly [`NUM_BUCKETS`] entries and the owners are **dense** (every ordinal in
    /// `0..max` is used — no empty shard, which would mean a shard owns nothing to serve).
    pub fn from_owners(owners: Vec<u32>) -> Result<Self, String> {
        if owners.len() != NUM_BUCKETS as usize {
            return Err(format!(
                "bucket map must have exactly {NUM_BUCKETS} entries, got {}",
                owners.len()
            ));
        }
        let max = owners.iter().copied().max().unwrap_or(0);
        let mut seen = vec![false; max as usize + 1];
        for &o in &owners {
            seen[o as usize] = true;
        }
        if let Some(missing) = seen.iter().position(|s| !s) {
            return Err(format!(
                "bucket map is not dense: shard {missing} owns no buckets"
            ));
        }
        Ok(Self { owners })
    }

    /// The shard ordinal owning bucket `b`.
    pub fn owner(&self, bucket: u32) -> u32 {
        self.owners[bucket as usize]
    }

    /// The owners table (`owners[b]` = shard for bucket `b`) — for persistence to the registry.
    pub fn owners(&self) -> &[u32] {
        &self.owners
    }

    /// The shard count (one past the max owner). A dense map serves every shard in `0..shards`.
    pub fn shards(&self) -> u32 {
        self.owners.iter().copied().max().map_or(1, |m| m + 1)
    }

    /// Buckets owned per shard, indexed by ordinal (`counts[s]`) — the input to skew diagnostics.
    pub fn counts(&self) -> Vec<u32> {
        let mut counts = vec![0u32; self.shards() as usize];
        for &o in &self.owners {
            counts[o as usize] += 1;
        }
        counts
    }

    /// A copy of this map with `bucket` reassigned to `shard` — the one-bucket move behind
    /// **skew relief**: shed a bucket from a busy shard to a quieter one without a full
    /// reshard. Errors if `bucket`/`shard` is out of range, or if the move would leave `bucket`'s
    /// old owner with no buckets (an empty shard isn't a valid dense map — move a different one).
    pub fn with_owner(&self, bucket: u32, shard: u32) -> Result<Self, String> {
        if bucket >= NUM_BUCKETS {
            return Err(format!(
                "bucket {bucket} is out of range (0..{NUM_BUCKETS})"
            ));
        }
        if shard >= self.shards() {
            return Err(format!(
                "shard {shard} is out of range (0..{})",
                self.shards()
            ));
        }
        let mut owners = self.owners.clone();
        owners[bucket as usize] = shard;
        Self::from_owners(owners)
    }

    /// Recommend a single-bucket **skew-relief** move from `per_shard_docs` — the per-shard load
    /// (e.g. doc counts) the diagnostics report, aligned to shard ordinals.
    /// Returns `(bucket, from, to)` to move the hottest shard's first bucket onto the coldest, or
    /// `None` when the load is already balanced (the hottest carries ≤ 10% more than the coldest)
    /// or the hot shard owns only one bucket (can't shed without emptying). Per-bucket *heat* isn't
    /// tracked, so it sheds a bucket from the hot shard rather than provably *the* hottest bucket.
    pub fn recommend_skew_move(&self, per_shard_docs: &[u64]) -> Option<(u32, u32, u32)> {
        if per_shard_docs.len() != self.shards() as usize || per_shard_docs.len() < 2 {
            return None;
        }
        let (from, &max) = per_shard_docs.iter().enumerate().max_by_key(|(_, &d)| d)?;
        let (to, &min) = per_shard_docs.iter().enumerate().min_by_key(|(_, &d)| d)?;
        if from == to || max <= min + min / 10 {
            return None; // already within 10% — not worth a migration
        }
        let (from, to) = (from as u32, to as u32);
        if self.counts()[from as usize] <= 1 {
            return None; // can't shed the hot shard's only bucket
        }
        let bucket = (0..NUM_BUCKETS).find(|&b| self.owner(b) == from)?;
        Some((bucket, from, to))
    }

    /// Plan a **reassignment** to `new_shards` (clamped ≥ 1) moving a **bounded** set of buckets:
    /// every bucket that can stay put does, and only the minimum needed to rebalance moves. On
    /// growth the new shards pull their share off the busiest owners; on shrink the removed
    /// shards' buckets spread onto the survivors. The result is balanced to within one bucket and
    /// deterministic (buckets considered in ascending order), so both languages plan identically.
    pub fn reassign(&self, new_shards: u32) -> Reassignment {
        let new_shards = new_shards.max(1);
        // Target per shard: floor(B/n), with the first `rem` shards taking one extra.
        let base = NUM_BUCKETS / new_shards;
        let rem = NUM_BUCKETS % new_shards;
        let target = |s: u32| base + u32::from(s < rem);

        let mut owners = self.owners.clone();
        let mut held = vec![0u32; new_shards as usize]; // buckets kept by each surviving shard
        let mut displaced: Vec<u32> = Vec::new(); // buckets that must move to a new owner

        // First pass: keep a bucket on its current owner if that shard survives and isn't full.
        for (b, &owner) in self.owners.iter().enumerate() {
            if owner < new_shards && held[owner as usize] < target(owner) {
                held[owner as usize] += 1;
            } else {
                displaced.push(b as u32);
            }
        }

        // Second pass: hand displaced buckets to shards still under target, lowest ordinal first.
        let mut next = 0u32;
        let mut moved = Vec::with_capacity(displaced.len());
        for b in displaced {
            while held[next as usize] >= target(next) {
                next += 1;
            }
            held[next as usize] += 1;
            let from = self.owners[b as usize];
            owners[b as usize] = next;
            moved.push((b, from, next));
        }

        Reassignment {
            map: Self { owners },
            moved,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{DocOp, LocatedDoc};
    use crate::doc::{Document, SourceCheckpoint, Value};
    use std::collections::BTreeMap;

    fn key(partition: &[(&str, &str)], id: &str) -> CompositeKey {
        CompositeKey::new(
            partition
                .iter()
                .map(|(k, v)| ((*k).into(), Value::from(*v)))
                .collect(),
            vec![("id".into(), Value::from(id))],
        )
    }

    fn upsert(k: CompositeKey) -> DocOp {
        DocOp::Upsert(LocatedDoc {
            doc: Document::new(k, BTreeMap::new()),
            iceberg_file: "f".into(),
            row_position: 0,
        })
    }

    /// Golden routing vectors — the **cross-language contract** between this `ShardRouter`
    /// and the JVM connector's port (`connector/.../ShardRouter.java`). Each row is
    /// `(name, partition fields, identifier fields, fnv1a(encode()), route@hashed(8),
    /// route@partitioned(8))`. The Java connector asserts the **same** numbers in
    /// `ShardRouterParityTest`, so a drift in either side's key encoding, hash, or strategy
    /// breaks a test rather than silently misplacing writes relative to reads.
    ///
    /// Regenerate after any encoding change by temporarily printing `fnv1a(k.encode())`, both
    /// `route`s, and `bucket()` for these keys, then update both this table and the Java one.
    /// Columns: `(name, key, fnv1a(encode), bucket@1024, route@hashed(8), route@partitioned(8))`.
    fn golden_cases() -> Vec<(&'static str, CompositeKey, u64, u32, u32, u32)> {
        use crate::doc::Value as V;
        let ck = |p: Vec<(&str, V)>, i: Vec<(&str, V)>| {
            CompositeKey::new(
                p.into_iter().map(|(n, v)| (n.to_string(), v)).collect(),
                i.into_iter().map(|(n, v)| (n.to_string(), v)).collect(),
            )
        };
        vec![
            (
                "k1",
                ck(vec![], vec![("id", V::from("doc-1"))]),
                15711809279696988285,
                125,
                5,
                5,
            ),
            (
                "k2",
                ck(vec![], vec![("id", V::from("doc-2"))]),
                15711805981162103652,
                868,
                4,
                4,
            ),
            (
                "k3",
                ck(
                    vec![("region", V::from("eu"))],
                    vec![("id", V::from("doc-1"))],
                ),
                3928395953618384062,
                190,
                6,
                0,
            ),
            (
                "k4",
                ck(
                    vec![("region", V::from("us"))],
                    vec![("id", V::from("doc-1"))],
                ),
                16431083332530908888,
                728,
                0,
                2,
            ),
            (
                "k5",
                ck(vec![], vec![("id", V::Int(42))]),
                3679709532207596177,
                657,
                1,
                1,
            ),
            (
                "k6",
                ck(vec![], vec![("id", V::Float(3.5))]),
                17979679306423702932,
                404,
                4,
                4,
            ),
            (
                "k7",
                ck(vec![], vec![("active", V::Bool(true))]),
                9588892399979799016,
                488,
                0,
                0,
            ),
            (
                "k8",
                ck(
                    vec![("region", V::from("eu")), ("tier", V::Int(2))],
                    vec![("id", V::from("x")), ("seq", V::Int(7))],
                ),
                11535628752453408818,
                50,
                2,
                4,
            ),
            // Temporal keys: `Ts` encodes under type tag 5 (canonical epoch micros,
            // 8-byte LE). One identifier-role and one partition-role vector, so a drift in the
            // Java side's tag-5 encoding fails the parity test under either strategy.
            (
                "ts_id",
                ck(vec![], vec![("ts", V::Ts(1_782_000_123_456_789))]),
                9199418800307739891,
                243,
                3,
                3,
            ),
            (
                "ts_part",
                ck(
                    vec![("day", V::Ts(1_782_000_000_000_000))],
                    vec![("id", V::from("doc-1"))],
                ),
                3480278431324234352,
                624,
                0,
                2,
            ),
            // Edge cases: a **partition-strategy key with no partition fields** must
            // fall back to hashing the full key (so part8 == hash8), and a **fully empty key**
            // must encode to empty bytes → fnv offset basis. Both must agree cross-language.
            (
                "empty_part",
                ck(vec![], vec![("id", V::from("solo"))]),
                18339136671911204773,
                933,
                5,
                5,
            ),
            (
                "empty_key",
                ck(vec![], vec![]),
                14695981039346656037,
                805,
                5,
                5,
            ),
        ]
    }

    #[test]
    fn route_matches_the_cross_language_golden_vectors() {
        let hashed8 = ShardRouter::hashed(8);
        let part8 = ShardRouter::partitioned(8);
        // A bucketed router over the balanced(8) map must agree with legacy hashed(8), since
        // 8 divides NUM_BUCKETS — the property that lets a legacy index adopt buckets for free.
        let bucketed8 = ShardRouter::bucketed(RoutingStrategy::Hash, BucketMap::balanced(8));
        for (name, key, fnv, bucket, hash8, partition8) in golden_cases() {
            assert_eq!(fnv1a(&key.encode()), fnv, "{name}: fnv1a(encode) drifted");
            assert_eq!(hashed8.bucket(&key), bucket, "{name}: bucket drifted");
            assert_eq!(hashed8.route(&key), hash8, "{name}: hash routing drifted");
            assert_eq!(
                part8.route(&key),
                partition8,
                "{name}: partition routing drifted"
            );
            assert_eq!(
                bucketed8.route(&key),
                hash8,
                "{name}: bucketed balanced(8) must match legacy hashed(8)"
            );
        }
    }

    #[test]
    fn route_is_deterministic_and_in_range() {
        let r = ShardRouter::hashed(4);
        for id in ["a", "b", "c", "d", "e", "f"] {
            let k = key(&[], id);
            let shard = r.route(&k);
            assert!(shard < 4);
            assert_eq!(shard, r.route(&k)); // stable
        }
        // A single shard always routes to 0.
        assert_eq!(ShardRouter::hashed(1).route(&key(&[], "x")), 0);
    }

    #[test]
    fn hash_routing_spreads_keys() {
        // Many keys land on more than one shard (not all in a single bucket).
        let r = ShardRouter::hashed(8);
        let used: std::collections::BTreeSet<u32> = (0..200)
            .map(|i| r.route(&key(&[], &format!("k{i}"))))
            .collect();
        assert!(used.len() > 1, "hash routing collapsed to one shard");
    }

    #[test]
    fn partition_routing_co_locates_a_partition() {
        let r = ShardRouter::partitioned(8);
        // Same partition value → same shard, regardless of identifier.
        let s1 = r.route(&key(&[("region", "eu")], "1"));
        let s2 = r.route(&key(&[("region", "eu")], "2"));
        let s3 = r.route(&key(&[("region", "eu")], "3"));
        assert_eq!(s1, s2);
        assert_eq!(s2, s3);

        // Different partitions generally spread across shards.
        let regions = ["eu", "us", "ap", "sa", "af", "me"];
        let used: std::collections::BTreeSet<u32> = regions
            .iter()
            .map(|reg| r.route(&key(&[("region", reg)], "1")))
            .collect();
        assert!(used.len() > 1, "partition routing collapsed to one shard");
    }

    #[test]
    fn partition_batch_returns_one_batch_per_shard_with_checkpoint_and_id() {
        let r = ShardRouter::hashed(4);
        let batch = CommitBatch::new(
            (0..10)
                .map(|i| upsert(key(&[], &format!("k{i}"))))
                .collect(),
            SourceCheckpoint::iceberg(42),
            "batch-A",
        );
        let parts = r.partition_batch(&batch);

        assert_eq!(parts.len(), 4, "one sub-batch per shard");
        for (ordinal, part) in parts.iter().enumerate() {
            assert_eq!(part.batch_id, format!("batch-A#s{ordinal}"));
            assert_eq!(part.checkpoint, SourceCheckpoint::iceberg(42));
        }
        // Every op is preserved exactly once across the sub-batches.
        let total: usize = parts.iter().map(|p| p.ops.len()).sum();
        assert_eq!(total, 10);
    }

    #[test]
    fn partition_batch_carries_from_and_safe_checkpoints_onto_every_sub_batch() {
        let r = ShardRouter::hashed(3);
        let batch = CommitBatch::new(
            vec![upsert(key(&[], "k1"))],
            SourceCheckpoint::iceberg(42),
            "batch-A",
        )
        .with_from_checkpoint(Some(SourceCheckpoint::iceberg(41)))
        .with_safe_checkpoint(Some(SourceCheckpoint::iceberg(40)));

        for part in r.partition_batch(&batch) {
            assert_eq!(
                part.from_checkpoint,
                Some(SourceCheckpoint::iceberg(41)),
                "sub-batch dropped the continuity `from` checkpoint"
            );
            assert_eq!(
                part.safe_checkpoint,
                Some(SourceCheckpoint::iceberg(40)),
                "sub-batch dropped the prune floor — its shard would never prune"
            );
        }
    }

    #[test]
    fn partition_batch_places_each_op_on_the_shard_route_picks() {
        let r = ShardRouter::hashed(8);
        let batch = CommitBatch::new(
            (0..50)
                .map(|i| upsert(key(&[], &format!("k{i}"))))
                .collect(),
            SourceCheckpoint::iceberg(1),
            "b",
        );
        let parts = r.partition_batch(&batch);
        for (ordinal, part) in parts.iter().enumerate() {
            for op in &part.ops {
                assert_eq!(
                    r.route(op.key()) as usize,
                    ordinal,
                    "op landed on a shard the router would not pick"
                );
            }
        }
    }

    #[test]
    fn partition_batch_preserves_op_order_within_a_shard() {
        let r = ShardRouter::hashed(3);
        let ops: Vec<DocOp> = (0..30)
            .map(|i| upsert(key(&[], &format!("k{i}"))))
            .collect();
        let batch = CommitBatch::new(ops.clone(), SourceCheckpoint::iceberg(1), "b");
        let parts = r.partition_batch(&batch);

        // Within each shard, ops appear in the same relative order as in the source batch.
        let original_order: Vec<&CompositeKey> = ops.iter().map(DocOp::key).collect();
        for part in &parts {
            let mut last = None;
            for op in &part.ops {
                let pos = original_order.iter().position(|k| *k == op.key()).unwrap();
                if let Some(prev) = last {
                    assert!(pos > prev, "op order within a shard was not preserved");
                }
                last = Some(pos);
            }
        }
    }

    #[test]
    fn balanced_map_is_even_and_dense() {
        for shards in [1u32, 2, 3, 5, 7, 8, 16, 100] {
            let map = BucketMap::balanced(shards);
            assert_eq!(map.shards(), shards);
            let counts = map.counts();
            assert_eq!(counts.len(), shards as usize);
            let (min, max) = (*counts.iter().min().unwrap(), *counts.iter().max().unwrap());
            assert!(max - min <= 1, "shards={shards}: uneven balance {counts:?}");
            assert_eq!(counts.iter().sum::<u32>(), NUM_BUCKETS);
            // Dense → round-trips through validation.
            assert!(BucketMap::from_owners(map.owners().to_vec()).is_ok());
        }
    }

    #[test]
    fn from_owners_rejects_wrong_length_and_gaps() {
        assert!(BucketMap::from_owners(vec![0; 10]).is_err()); // wrong length
        let mut owners = vec![0u32; NUM_BUCKETS as usize];
        owners[0] = 2; // shard 1 owns nothing → not dense
        assert!(BucketMap::from_owners(owners).is_err());
    }

    #[test]
    fn reassign_growth_moves_a_bounded_balanced_set() {
        // Grow 4 → 5: only the new shard's share should move (~1/5 of buckets), and the result
        // must be balanced and dense. Every non-moved bucket keeps its original owner.
        let before = BucketMap::balanced(4);
        let r = before.reassign(5);

        // Balanced + dense over 5 shards.
        assert_eq!(r.map.shards(), 5);
        let counts = r.map.counts();
        assert!(counts.iter().max().unwrap() - counts.iter().min().unwrap() <= 1);

        // Bounded movement: at most ~B/5 buckets move (well under re-routing everything). The new
        // shard receives exactly its target share, and that's the lower bound on what must move.
        let target_new = (NUM_BUCKETS / 5) as usize; // 204
        assert!(
            r.moved.len() <= target_new + 5,
            "moved {} buckets, expected ~{target_new}",
            r.moved.len()
        );
        assert!(r.moved.len() >= target_new);

        // Non-moved buckets are unchanged; moved buckets actually changed owner.
        let moved_set: std::collections::BTreeSet<u32> =
            r.moved.iter().map(|(b, _, _)| *b).collect();
        for b in 0..NUM_BUCKETS {
            if moved_set.contains(&b) {
                assert_ne!(before.owner(b), r.map.owner(b));
            } else {
                assert_eq!(before.owner(b), r.map.owner(b));
            }
        }
        // The recorded `from`/`to` match the before/after maps.
        for (b, from, to) in &r.moved {
            assert_eq!(*from, before.owner(*b));
            assert_eq!(*to, r.map.owner(*b));
        }
    }

    #[test]
    fn reassign_shrink_redistributes_the_removed_shard() {
        // Shrink 5 → 4: shard 4's buckets must all move onto 0..4, balanced, and no bucket that
        // already lived on 0..4 needs to move (they're kept).
        let before = BucketMap::balanced(5);
        let r = before.reassign(4);
        assert_eq!(r.map.shards(), 4);
        let counts = r.map.counts();
        assert!(counts.iter().max().unwrap() - counts.iter().min().unwrap() <= 1);
        // Exactly the buckets that lived on the removed shard 4 move.
        for (b, from, _) in &r.moved {
            assert_eq!(*from, 4, "only the removed shard's bucket {b} should move");
        }
        assert_eq!(r.moved.len() as u32, before.counts()[4]);
    }

    #[test]
    fn from_registry_picks_legacy_or_bucketed_and_the_map_wins() {
        // Empty bucket map ⇒ legacy router over the assigned shard count.
        let legacy = ShardRouter::from_registry(RoutingStrategy::Hash, &[], 4).unwrap();
        assert_eq!(legacy, ShardRouter::hashed(4));

        // A balanced(4) map ⇒ bucketed router that matches legacy hashed(4) (4 | NUM_BUCKETS).
        let owners = BucketMap::balanced(4).owners().to_vec();
        let bucketed = ShardRouter::from_registry(RoutingStrategy::Hash, &owners, 4).unwrap();
        for id in ["a", "b", "c", "d", "e", "f", "g"] {
            assert_eq!(
                bucketed.route(&key(&[], id)),
                ShardRouter::hashed(4).route(&key(&[], id))
            );
        }

        // Mid-grow, the assigned count runs AHEAD of the routed count (build targets register
        // before the cutover): the map wins, keys keep routing over the current 4 shards, and a
        // gateway starting during the rebuild window can still construct routing.
        let mid_grow = ShardRouter::from_registry(RoutingStrategy::Hash, &owners, 5).unwrap();
        assert_eq!(mid_grow.shards(), 4);
        for id in ["a", "b", "c"] {
            assert_eq!(
                mid_grow.route(&key(&[], id)),
                ShardRouter::hashed(4).route(&key(&[], id))
            );
        }
        // A malformed map (wrong length) is still rejected.
        assert!(ShardRouter::from_registry(RoutingStrategy::Hash, &[0, 1, 2], 3).is_err());
    }

    #[test]
    fn reshard_relocates_only_moved_buckets_and_loses_nothing() {
        // The cutover-correctness property: across a 2→3 reshard, every key still has
        // exactly one owning shard (no lost/duplicate docs), a key relocates **iff** its bucket
        // moved (minimal movement), and a relocated key lands on its bucket's new owner — i.e. the
        // rebuilt shards (split by `route`) and the post-cutover read routing agree.
        let before = BucketMap::balanced(2);
        let plan = before.reassign(3);
        let r_old = ShardRouter::bucketed(RoutingStrategy::Hash, before);
        let r_new = ShardRouter::bucketed(RoutingStrategy::Hash, plan.map.clone());
        let moved: std::collections::BTreeSet<u32> =
            plan.moved.iter().map(|(b, _, _)| *b).collect();

        let keys: Vec<CompositeKey> = (0..500).map(|i| key(&[], &format!("k{i}"))).collect();
        let mut relocated = 0;
        for k in &keys {
            let (old_shard, new_shard) = (r_old.route(k), r_new.route(k));
            assert!(old_shard < 2 && new_shard < 3, "shard out of range");
            // A key's bucket is placement-independent, so old and new routers agree on it.
            let bucket = r_old.bucket(k);
            assert_eq!(bucket, r_new.bucket(k));
            if moved.contains(&bucket) {
                assert_eq!(
                    new_shard,
                    plan.map.owner(bucket),
                    "moved key landed on wrong owner"
                );
                if old_shard != new_shard {
                    relocated += 1;
                }
            } else {
                assert_eq!(
                    old_shard, new_shard,
                    "an unmoved bucket's key changed shard"
                );
            }
        }
        assert!(relocated > 0, "reshard relocated nothing");

        // `owns` is the per-shard rebuild filter: a key is kept by exactly one shard's rebuild.
        for k in &keys {
            let kept_by: Vec<u32> = (0..3).filter(|s| r_new.owns(k, *s)).collect();
            assert_eq!(
                kept_by,
                vec![r_new.route(k)],
                "a key is owned by exactly one shard"
            );
        }
    }

    #[test]
    fn with_owner_relocates_one_bucket_and_guards_emptying() {
        let map = BucketMap::balanced(4);
        let owner0 = map.owner(0);
        let other = (owner0 + 1) % 4;
        let moved = map.with_owner(0, other).unwrap();
        assert_eq!(moved.owner(0), other);
        // Only bucket 0 changed.
        for b in 1..NUM_BUCKETS {
            assert_eq!(moved.owner(b), map.owner(b));
        }
        // Out-of-range bucket / shard are rejected.
        assert!(map.with_owner(NUM_BUCKETS, 0).is_err());
        assert!(map.with_owner(0, 4).is_err());
        // Emptying a **middle** shard leaves a gap (shard 1 owns nothing) → rejected. Build a
        // 3-shard map where shard 1 owns a single bucket, then move that bucket away.
        let mut owners = vec![0u32; NUM_BUCKETS as usize];
        owners[5] = 1;
        owners[7] = 2;
        let lopsided = BucketMap::from_owners(owners).unwrap();
        assert_eq!(lopsided.shards(), 3);
        assert!(lopsided.with_owner(5, 0).is_err());
    }

    #[test]
    fn recommend_skew_move_sheds_from_the_hottest_shard() {
        let map = BucketMap::balanced(3);
        // Shard 0 is hot (1000 docs), shard 2 is cold (100) → move a bucket 0 → 2.
        let (bucket, from, to) = map.recommend_skew_move(&[1000, 500, 100]).unwrap();
        assert_eq!((from, to), (0, 2));
        assert_eq!(
            map.owner(bucket),
            0,
            "the moved bucket is owned by the hot shard"
        );

        // Balanced-enough load (within 10%) → no move.
        assert!(map.recommend_skew_move(&[100, 100, 105]).is_none());
        // Wrong-length load vector → no move.
        assert!(map.recommend_skew_move(&[100, 100]).is_none());

        // Applying the recommendation actually shifts the bucket.
        let relieved = map.with_owner(bucket, to).unwrap();
        assert_eq!(relieved.owner(bucket), to);
        assert_eq!(relieved.counts()[0], map.counts()[0] - 1);
    }

    #[test]
    fn bucketed_router_uses_the_map_owner() {
        // A hand-built map that sends every bucket to shard 0 collapses routing to shard 0.
        let map = BucketMap::from_owners(vec![0; NUM_BUCKETS as usize]).unwrap();
        let r = ShardRouter::bucketed(RoutingStrategy::Hash, map);
        assert_eq!(r.shards(), 1);
        for id in ["a", "b", "c", "z"] {
            assert_eq!(r.route(&key(&[], id)), 0);
        }
    }

    #[test]
    fn partition_batch_routes_deletes_by_their_key() {
        let r = ShardRouter::hashed(4);
        // An upsert and a later delete of the *same* key must land on the same shard, in order.
        let k = key(&[], "doc-1");
        let batch = CommitBatch::new(
            vec![upsert(k.clone()), DocOp::Delete(k.clone())],
            SourceCheckpoint::iceberg(1),
            "b",
        );
        let parts = r.partition_batch(&batch);
        let owner = r.route(&k) as usize;
        assert_eq!(
            parts[owner].ops.len(),
            2,
            "both ops for a key share a shard"
        );
        // Order within the shard: upsert before delete.
        assert!(matches!(parts[owner].ops[0], DocOp::Upsert(_)));
        assert!(matches!(parts[owner].ops[1], DocOp::Delete(_)));
        // No other shard received anything.
        let elsewhere: usize = parts
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != owner)
            .map(|(_, p)| p.ops.len())
            .sum();
        assert_eq!(elsewhere, 0);
    }
}
