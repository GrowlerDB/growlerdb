//! The GrowlerDB **Control Plane**: the cluster's lightweight source of truth for index
//! definitions + status (the [`Registry`]), the shard map ([`ShardAssignment`]), and per-shard
//! leader election. Not in the hot path of search/write — only consulted for routing and topology.

mod backend;
#[cfg(feature = "postgres")]
mod postgres_backend;
mod registry;

pub use backend::{JsonFileBackend, PersistedState, RegistryBackend, RegistrySnapshot};
#[cfg(feature = "postgres")]
pub use postgres_backend::PostgresBackend;
pub use registry::{
    glob_match, ActivityEvent, ApiToken, IndexEntry, IndexStatus, IndexSummary, NodeId, Registry,
    RegistryError, Result, SavedQuery, ShardAssignment, Unit, UnitHolders, WindowAnnounce,
    WindowAssignment, NODE_HEARTBEAT_TTL_MS, NODE_REANNOUNCE_INTERVAL_MS,
};
