//! Small shared helpers for the gRPC service impls: map errors to a tonic
//! [`Status`] and run blocking work off the async runtime. These replace the
//! `|e| to_status(Code::Internal, WireError::new("INTERNAL", e.to_string()))` closures and the
//! `spawn_blocking(..).await.map_err(..)` boilerplate that was copy-pasted across
//! `{search,suggest,lookup,admin}_service.rs`.

use growlerdb_proto::to_status;
use growlerdb_proto::v1::Error as WireError;
use tonic::{Code, Status};

/// Map any `Display` error to an `Internal` tonic [`Status`] with the standard `INTERNAL` wire code.
pub(crate) fn internal(e: impl std::fmt::Display) -> Status {
    to_status(Code::Internal, WireError::new("INTERNAL", e.to_string()))
}

/// The "index not served by this node" guard shared by the single-index Node RPCs:
/// an empty `requested` name means "the served index"; any other name is `NotFound`. Replaces the
/// `if !req.index.is_empty() && req.index != self.index { return Err(NotFound…) }` block copy-pasted
/// across the Admin service (and mirrored on the gateway).
pub(crate) fn check_served(requested: &str, served: &str) -> Result<(), Status> {
    if !requested.is_empty() && requested != served {
        return Err(to_status(
            Code::NotFound,
            WireError::new(
                "NOT_FOUND",
                format!("index `{requested}` is not served by this node"),
            ),
        ));
    }
    Ok(())
}

/// Run blocking work on the blocking pool, mapping a `JoinError` (a panicked/cancelled task) to an
/// `Internal` [`Status`]. The closure's own return value is passed through untouched, so a closure
/// returning `Result<T, E>` yields `Result<Result<T, E>, Status>` — the caller handles the inner
/// error with its own mapping (often [`internal`]).
pub(crate) async fn run_blocking<T, F>(f: F) -> Result<T, Status>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f).await.map_err(internal)
}
