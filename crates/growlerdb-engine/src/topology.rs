//! Turning a Control-Plane **shard map** into the ordered shard set a [`Gateway`] fronts.
//!
//! The [`Gateway`](crate::Gateway) scatter-gathers over `Vec<Arc<dyn Node>>` where the
//! **vector index is the shard ordinal** the [`ShardRouter`](growlerdb_core::ShardRouter)
//! routes to. The Control-Plane [`Registry`](growlerdb_controlplane::Registry) owns the
//! authoritative placement — `shard ordinal -> `[`ShardAssignment`] (primary + replicas).
//! [`shard_primaries`] bridges the two: it validates the map describes a complete `0..N`
//! shard set with a primary on every shard, and returns the primaries **ordered by ordinal**
//! so the caller can resolve each to a [`Node`](crate::Node) (e.g. via
//! [`RemoteNode::connect`](crate::RemoteNode::connect)) and hand the list to
//! [`Gateway::sharded`](crate::Gateway::sharded).

use std::collections::BTreeMap;

use growlerdb_controlplane::{NodeId, ShardAssignment};

/// Why a shard map can't be turned into a Gateway shard set. Each variant is a placement
/// invariant the Gateway's scatter-gather depends on (ordinal = vector index, one primary
/// per shard) — surfaced loudly rather than silently dropping or misordering a shard.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ShardTopologyError {
    /// No shards are assigned yet — there is nothing to front.
    #[error("shard map is empty — no shards assigned")]
    Empty,
    /// Shard ordinals must be exactly `0..N` with no gaps; the Gateway indexes shards by
    /// ordinal, so a hole would misalign every shard above it.
    #[error("shard map is non-contiguous: expected shard {expected}, found {found}")]
    NonContiguous { expected: u32, found: u32 },
    /// A shard exists in the map but has no primary elected (only read replicas, or nothing).
    /// The Gateway routes writes and queries to the primary, so it cannot front this shard.
    #[error("shard {0} has no assigned primary")]
    UnassignedPrimary(u32),
}

/// The **primary** node of each shard, ordered by shard ordinal `0..N`.
///
/// Validates the map is a complete, contiguous `0..N` shard set with a primary on every
/// shard — the precondition [`Gateway::sharded`](crate::Gateway::sharded) assumes. Returns
/// the primaries in ordinal order so position `i` is shard `i`.
pub fn shard_primaries(
    shard_map: &BTreeMap<u32, ShardAssignment>,
) -> Result<Vec<NodeId>, ShardTopologyError> {
    if shard_map.is_empty() {
        return Err(ShardTopologyError::Empty);
    }
    // BTreeMap iterates in key order, so `expected` (0, 1, 2, …) must match each ordinal.
    let mut primaries = Vec::with_capacity(shard_map.len());
    for (expected, (&ordinal, assignment)) in shard_map.iter().enumerate() {
        let expected = expected as u32;
        if ordinal != expected {
            return Err(ShardTopologyError::NonContiguous {
                expected,
                found: ordinal,
            });
        }
        let primary = assignment
            .primary
            .clone()
            .ok_or(ShardTopologyError::UnassignedPrimary(ordinal))?;
        primaries.push(primary);
    }
    Ok(primaries)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assignment(primary: Option<&str>, replicas: &[&str]) -> ShardAssignment {
        ShardAssignment {
            primary: primary.map(NodeId::from),
            replicas: replicas.iter().map(|r| NodeId::from(*r)).collect(),
        }
    }

    #[test]
    fn empty_map_is_rejected() {
        let map = BTreeMap::new();
        assert_eq!(shard_primaries(&map), Err(ShardTopologyError::Empty));
    }

    #[test]
    fn primaries_are_returned_in_ordinal_order() {
        // Insert out of order; the result must still be ordinal-ordered.
        let mut map = BTreeMap::new();
        map.insert(2, assignment(Some("node-c"), &[]));
        map.insert(0, assignment(Some("node-a"), &["node-x"]));
        map.insert(1, assignment(Some("node-b"), &[]));

        let primaries = shard_primaries(&map).unwrap();
        assert_eq!(
            primaries,
            vec![
                NodeId::from("node-a"),
                NodeId::from("node-b"),
                NodeId::from("node-c"),
            ]
        );
    }

    #[test]
    fn a_gap_in_ordinals_is_non_contiguous() {
        let mut map = BTreeMap::new();
        map.insert(0, assignment(Some("node-a"), &[]));
        map.insert(2, assignment(Some("node-c"), &[])); // missing shard 1
        assert_eq!(
            shard_primaries(&map),
            Err(ShardTopologyError::NonContiguous {
                expected: 1,
                found: 2
            })
        );
    }

    #[test]
    fn a_shard_without_a_primary_is_rejected() {
        let mut map = BTreeMap::new();
        map.insert(0, assignment(Some("node-a"), &[]));
        map.insert(1, assignment(None, &["node-replica"])); // replicas but no primary
        assert_eq!(
            shard_primaries(&map),
            Err(ShardTopologyError::UnassignedPrimary(1))
        );
    }

    #[test]
    fn single_shard_is_valid() {
        let mut map = BTreeMap::new();
        map.insert(0, assignment(Some("only"), &[]));
        assert_eq!(shard_primaries(&map), Ok(vec![NodeId::from("only")]));
    }
}
