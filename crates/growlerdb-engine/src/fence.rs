//! The **reindex write-fence** ([task-71]): a clone-shared flag the [Admin](crate::AdminService)
//! service engages while a reindex rebuilds, and the [Write](crate::WriteService) service checks to
//! reject new writes with a retryable status. Fencing writes across the rebuild stops the connector
//! from advancing the shard past the rebuild's source snapshot — a delta the swap would otherwise
//! drop (regressing the checkpoint / breaking exactly-once, M3 review C3). It also doubles as the
//! single-flight guard: a second reindex can't [`engage`](ReindexFence::engage) an engaged fence.
//!
//! [task-71]: ../../../backlog/tasks/task-71%20-%20Reindex%20and%20alter%20robustness.md

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// A shared reindex write-fence. Cheap to clone (an `Arc<AtomicBool>`); all clones see the same
/// state, so the Admin and Write services share one fence.
#[derive(Clone, Default)]
pub struct ReindexFence(Arc<AtomicBool>);

impl ReindexFence {
    /// A new, open (not reindexing) fence.
    pub fn new() -> Self {
        Self::default()
    }

    /// Engage the fence at the start of a reindex. Returns `true` if it was open (this caller now
    /// owns the reindex), `false` if a reindex is already in flight — the single-flight guard.
    pub fn engage(&self) -> bool {
        self.0
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Release the fence when the reindex finishes (or fails). Idempotent.
    pub fn release(&self) {
        self.0.store(false, Ordering::Release);
    }

    /// Whether a reindex is currently fencing writes.
    pub fn is_engaged(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// RAII release of an engaged [`ReindexFence`] — clears it on scope exit (success, early-return
/// error, or panic), so a failed reindex never leaves writes fenced forever.
pub struct ReindexGuard(ReindexFence);

impl ReindexGuard {
    /// Hold the guard for `fence` (assumed already [`engaged`](ReindexFence::engage)).
    pub fn new(fence: ReindexFence) -> Self {
        Self(fence)
    }
}

impl Drop for ReindexGuard {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engage_is_single_flight_and_guard_releases() {
        let fence = ReindexFence::new();
        assert!(!fence.is_engaged());

        assert!(fence.engage(), "first engage succeeds");
        assert!(fence.is_engaged());
        assert!(!fence.engage(), "second engage is refused while engaged");

        {
            let _guard = ReindexGuard::new(fence.clone());
            assert!(fence.is_engaged());
        } // guard dropped here
        assert!(!fence.is_engaged(), "guard released the fence on drop");
        assert!(fence.engage(), "fence is reusable after release");
    }
}
