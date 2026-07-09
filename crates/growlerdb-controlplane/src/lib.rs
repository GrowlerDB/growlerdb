//! The GrowlerDB **Control Plane** ([task-28], M3 Phase B2): the cluster's lightweight
//! source of truth for index definitions + status (the **registry**), the shard map, and
//! per-shard leader election. It is *not* in the hot path of search/write — only consulted
//! for routing and topology.
//!
//! The [`Registry`] is the cluster's catalog: index definitions + lifecycle status (create /
//! drop / list, completing [task-26]'s lifecycle) and the per-shard **shard map**
//! ([`ShardAssignment`] — primary/replica per shard, with replica promotion on primary loss).
//! Durably backed by a JSON document with atomic writes. Leader election (K8s leases driving
//! that promotion) joins it in a later slice.
//!
//! [task-28]: ../../../design/06-service-architecture.md
//! [task-26]: ../../../backlog/tasks/task-26%20-%20Cached-field%20policy%20+%20index%20admin.md

mod registry;

pub use registry::{
    glob_match, ActivityEvent, ApiToken, IndexEntry, IndexStatus, IndexSummary, NodeId, Registry,
    RegistryError, Result, SavedQuery, ShardAssignment, WindowAssignment,
};
