//! The GrowlerDB **Control Plane**: the cluster's lightweight source of truth for index
//! definitions + status (the **registry**), the shard map, and per-shard leader election. It
//! is *not* in the hot path of search/write — only consulted for routing and topology.
//!
//! The [`Registry`] is the cluster's catalog: index definitions + lifecycle status (create /
//! drop / list) and the per-shard **shard map** ([`ShardAssignment`] — primary/replica per
//! shard, with replica promotion on primary loss). Durably backed by a JSON document with
//! atomic writes.

mod registry;

pub use registry::{
    glob_match, ActivityEvent, ApiToken, IndexEntry, IndexStatus, IndexSummary, NodeId, Registry,
    RegistryError, Result, SavedQuery, ShardAssignment, WindowAssignment,
};
