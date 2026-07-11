//! A lazily-connected, shared [`IcebergReader`] handle.
//!
//! Connecting `IcebergReader::connect` per RPC would rebuild the REST-catalog client
//! (auth/config/HTTP client) for every `GetByKey`. A long-lived service instead holds a
//! [`SharedReader`]: the first request connects, later requests
//! reuse the same reader (and with it the reader's snapshot-pinned
//! [plan cache](crate::plan_cache)). On a source failure the holder calls
//! [`invalidate`](SharedReader::invalidate), so a dead/expired client is dropped and the
//! **next** request reconnects — a broken client is never cached forever, and a failed
//! connect leaves the slot empty (the next call simply retries).

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{IcebergConfig, IcebergReader, Result};

/// The generic lazy slot behind [`SharedReader`], kept separate so the
/// connect/retry/invalidate contract is unit-testable with a counting connector
/// (a real `IcebergReader::connect` needs a catalog endpoint).
pub(crate) struct LazySlot<T> {
    slot: Mutex<Option<Arc<T>>>,
}

impl<T> LazySlot<T> {
    pub(crate) fn new() -> Self {
        Self {
            slot: Mutex::new(None),
        }
    }

    /// The held value, or connect one via `connect` and hold it. The lock **is** held
    /// across the connect await — deliberately: concurrent cold-start callers coalesce
    /// into one connect instead of racing N of them. A failed connect stores nothing,
    /// so the next caller retries.
    pub(crate) async fn get_or_connect<F, Fut, E>(
        &self,
        connect: F,
    ) -> std::result::Result<Arc<T>, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, E>>,
    {
        let mut slot = self.slot.lock().await;
        if let Some(held) = &*slot {
            return Ok(Arc::clone(held));
        }
        let connected = Arc::new(connect().await?);
        *slot = Some(Arc::clone(&connected));
        Ok(connected)
    }

    /// Drop the held value; the next [`get_or_connect`](Self::get_or_connect) reconnects.
    pub(crate) async fn invalidate(&self) {
        *self.slot.lock().await = None;
    }
}

/// A shared, reconnect-on-failure handle to one catalog's [`IcebergReader`].
/// See the [module docs](self).
pub struct SharedReader {
    cfg: IcebergConfig,
    slot: LazySlot<IcebergReader>,
}

impl SharedReader {
    /// A handle that will lazily connect to the catalog described by `cfg`.
    pub fn new(cfg: IcebergConfig) -> Self {
        Self {
            cfg,
            slot: LazySlot::new(),
        }
    }

    /// The connected reader — connecting on first use (or after an
    /// [`invalidate`](Self::invalidate)). A connect failure is returned and cached
    /// **nowhere**: the next call retries.
    pub async fn get(&self) -> Result<Arc<IcebergReader>> {
        self.slot
            .get_or_connect(|| IcebergReader::connect(&self.cfg))
            .await
    }

    /// Drop the held reader (and its plan cache) after a source failure, so the next
    /// request reconnects with fresh credentials/config instead of reusing a client
    /// that may be dead.
    pub async fn invalidate(&self) {
        self.slot.invalidate().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn a_failed_connect_does_not_poison_the_slot() {
        let slot: LazySlot<u32> = LazySlot::new();
        let connects = AtomicUsize::new(0);
        let err = slot
            .get_or_connect(|| async {
                connects.fetch_add(1, Ordering::SeqCst);
                Err::<u32, _>("no route to catalog".to_string())
            })
            .await
            .unwrap_err();
        assert_eq!(err, "no route to catalog");
        // The failure was not cached: the next call retries the connect and succeeds.
        let v = slot
            .get_or_connect(|| async {
                connects.fetch_add(1, Ordering::SeqCst);
                Ok::<_, String>(7)
            })
            .await
            .unwrap();
        assert_eq!(*v, 7);
        assert_eq!(connects.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_successful_connect_is_reused_until_invalidated() {
        let slot: LazySlot<u32> = LazySlot::new();
        let connects = AtomicUsize::new(0);
        let connect = || async {
            connects.fetch_add(1, Ordering::SeqCst);
            Ok::<_, String>(7)
        };
        let a = slot.get_or_connect(connect).await.unwrap();
        let b = slot.get_or_connect(connect).await.unwrap();
        assert!(Arc::ptr_eq(&a, &b), "the same connection is shared");
        assert_eq!(
            connects.load(Ordering::SeqCst),
            1,
            "one connect for many calls"
        );

        // After an invalidation (a source failure), the next call reconnects.
        slot.invalidate().await;
        let c = slot.get_or_connect(connect).await.unwrap();
        assert!(!Arc::ptr_eq(&a, &c));
        assert_eq!(connects.load(Ordering::SeqCst), 2);
    }
}
