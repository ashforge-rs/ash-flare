//! Common types used throughout the supervision tree

use crate::restart::RestartPolicy;
use serde::{Deserialize, Serialize};

use dashmap::DashMap;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use std::sync::Arc;

/// Result of a worker's execution
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[rkyv(derive(Debug))]
#[non_exhaustive]
pub enum ChildExitReason {
    /// Normal termination
    Normal,
    /// Abnormal termination (error or panic)
    Abnormal,
    /// Stopped by its supervisor, cooperatively
    Shutdown,
    /// Stopped by its supervisor by aborting its task, because its shutdown
    /// setting is `BrutalKill` or it overran its shutdown timeout. Its
    /// `shutdown` hook did not run.
    Killed,
}

/// Child identifier type
pub type ChildId = String;

/// Information about a supervised child, as returned by
/// [`SupervisorHandle::which_children`](crate::SupervisorHandle::which_children)
/// and sent over the wire by the [distributed](crate::distributed) protocol.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
#[rkyv(derive(Debug))]
pub struct ChildInfo {
    /// Unique identifier for the child
    pub id: ChildId,
    /// Type of child (Worker or Supervisor)
    pub child_type: ChildType,
    /// Restart policy for the child; `None` for nested supervisors, whose
    /// policy is set with
    /// [`SupervisorSpec::with_restart_policy`](crate::SupervisorSpec::with_restart_policy)
    /// (`Permanent` unless changed).
    pub restart_policy: Option<RestartPolicy>,
}

/// Type of child in supervision tree
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[rkyv(derive(Debug))]
pub enum ChildType {
    /// A worker process
    Worker,
    /// A nested supervisor
    Supervisor,
}

/// Cooperative shutdown signal handed to a worker.
///
/// A supervisor asks a worker to stop before resorting to aborting its task.
/// Take the signal in the worker's factory (`|signal| MyWorker { signal }`) and
/// select on [`ShutdownSignal::cancelled`] inside [`run`](crate::Worker::run) so
/// the worker can return promptly and let
/// [`shutdown`](crate::Worker::shutdown) perform its cleanup.
///
/// A worker that ignores the signal is aborted once the shutdown timeout
/// elapses, and its `shutdown` hook does **not** run.
///
/// # Examples
///
/// ```
/// use ash_flare::{RestartPolicy, ShutdownSignal, SupervisorSpec, Worker};
///
/// struct Pump {
///     shutdown: ShutdownSignal,
/// }
///
/// impl Worker for Pump {
///     type Error = std::io::Error;
///
///     async fn run(&mut self) -> Result<(), Self::Error> {
///         loop {
///             tokio::select! {
///                 () = self.shutdown.cancelled() => return Ok(()),
///                 () = tokio::time::sleep(std::time::Duration::from_millis(50)) => {
///                     // ... do a unit of work ...
///                 }
///             }
///         }
///     }
/// }
///
/// // A factory that takes one argument receives the worker's signal.
/// let spec = SupervisorSpec::new("app")
///     .with_worker("pump", |shutdown| Pump { shutdown }, RestartPolicy::Permanent);
/// ```
#[derive(Clone, Debug)]
pub struct ShutdownSignal {
    rx: tokio::sync::watch::Receiver<bool>,
}

impl ShutdownSignal {
    pub(crate) fn new(rx: tokio::sync::watch::Receiver<bool>) -> Self {
        Self { rx }
    }

    /// Creates a signal that is never triggered.
    ///
    /// Useful for constructing a worker outside a supervision tree, such as in
    /// tests.
    #[must_use]
    pub fn never() -> Self {
        static NEVER: std::sync::OnceLock<tokio::sync::watch::Sender<bool>> =
            std::sync::OnceLock::new();
        // The sender is kept alive for the program's lifetime so `cancelled`
        // parks forever instead of returning on a closed channel.
        let tx = NEVER.get_or_init(|| tokio::sync::watch::channel(false).0);
        Self { rx: tx.subscribe() }
    }

    /// Returns `true` if shutdown has already been requested.
    ///
    /// Like [`cancelled`](Self::cancelled), this also reports `true` once the
    /// supervisor side is gone, since nothing is left to supervise the worker.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        // `has_changed` only errors when every sender has been dropped.
        *self.rx.borrow() || self.rx.has_changed().is_err()
    }

    /// Resolves once shutdown has been requested.
    ///
    /// Cancel-safe, so it can be used directly as a `tokio::select!` branch.
    /// Returns immediately if shutdown was already requested.
    pub async fn cancelled(&self) {
        let mut rx = self.rx.clone();
        if *rx.borrow() {
            return;
        }
        // `changed` only errors when every sender is gone, which means the
        // supervisor is gone too; treat that as a shutdown request.
        while rx.changed().await.is_ok() {
            if *rx.borrow() {
                return;
            }
        }
    }
}

/// Process-local, concurrency-safe key-value store for sharing state between
/// workers.
///
/// Cloning is cheap and every clone sees the same data, so sharing state is a
/// matter of capturing a clone in each worker's factory. Because the context
/// lives outside the workers, its contents survive worker restarts.
///
/// ```
/// use ash_flare::{RestartPolicy, SupervisorSpec, Worker, WorkerContext};
///
/// struct Counter { ctx: WorkerContext }
///
/// impl Worker for Counter {
///     type Error = std::io::Error;
///     async fn run(&mut self) -> Result<(), Self::Error> {
///         self.ctx.update("count", |v| {
///             let n = v.and_then(|v| v.as_u64()).unwrap_or(0);
///             Some(serde_json::json!(n + 1))
///         });
///         Ok(())
///     }
/// }
///
/// let ctx = WorkerContext::new();
/// let spec = SupervisorSpec::new("app")
///     .with_worker("a", { let ctx = ctx.clone(); move || Counter { ctx: ctx.clone() } }, RestartPolicy::Transient)
///     .with_worker("b", { let ctx = ctx.clone(); move || Counter { ctx: ctx.clone() } }, RestartPolicy::Transient);
/// ```
///
/// The store is backed by `DashMap` for lock-free concurrent access.
///
/// # Performance
/// - Uses `DashMap` for lock-free concurrent operations
/// - Cloning is cheap (only clones the `Arc`, not the data)
#[derive(Clone, Debug)]
pub struct WorkerContext {
    store: Arc<DashMap<String, serde_json::Value>>,
}

impl WorkerContext {
    /// Creates a new empty `WorkerContext`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            store: Arc::new(DashMap::new()),
        }
    }

    /// Gets a value from the store by key.
    ///
    /// Returns `None` if the key doesn't exist.
    ///
    /// Note: This method clones the value. For read-heavy workloads,
    /// consider caching values or using primitives that are cheap to clone.
    #[inline]
    #[must_use]
    pub fn get(&self, key: &str) -> Option<serde_json::Value> {
        self.store.get(key).map(|entry| entry.value().clone())
    }

    /// Gets a value and applies a function to it without cloning.
    ///
    /// This is more efficient than `get()` when you only need to inspect the value.
    ///
    /// # Examples
    /// ```
    /// # use ash_flare::WorkerContext;
    /// let ctx = WorkerContext::new();
    /// ctx.set("count", serde_json::json!(42));
    /// let is_positive = ctx.with_value("count", |v| {
    ///     v.and_then(|val| val.as_i64()).map(|n| n > 0).unwrap_or(false)
    /// });
    /// assert!(is_positive);
    /// ```
    #[inline]
    pub fn with_value<F, R>(&self, key: &str, f: F) -> R
    where
        F: FnOnce(Option<&serde_json::Value>) -> R,
    {
        match self.store.get(key) {
            Some(entry) => f(Some(entry.value())),
            None => f(None),
        }
    }

    /// Sets a value in the store.
    #[inline]
    pub fn set(&self, key: impl Into<String>, value: serde_json::Value) {
        self.store.insert(key.into(), value);
    }

    /// Deletes a key from the store.
    ///
    /// Returns the previous value if it existed.
    #[inline]
    #[must_use]
    pub fn delete(&self, key: &str) -> Option<serde_json::Value> {
        self.store.remove(key).map(|(_, v)| v)
    }

    /// Returns the number of key-value pairs in the store.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.store.len()
    }

    /// Returns `true` if the store contains no key-value pairs.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }

    /// Checks if a key exists in the store without retrieving the value.
    #[inline]
    #[must_use]
    pub fn contains_key(&self, key: &str) -> bool {
        self.store.contains_key(key)
    }

    /// Updates a value in the store using a function.
    ///
    /// The function receives the current value (or `None` if the key doesn't exist)
    /// and returns an `Option` to control the update:
    /// - `Some(value)` - Insert or update the key with the new value
    /// - `None` - **Remove the key from the store** (if it existed)
    ///
    /// # Warning
    /// Returning `None` from the update function will **delete the key** from the store.
    /// This is useful for conditional removal but can lead to unexpected data loss if not intended.
    ///
    /// # Examples
    /// ```
    /// # use ash_flare::WorkerContext;
    /// let ctx = WorkerContext::new();
    ///
    /// // Increment a counter, initializing to 1 if it doesn't exist
    /// ctx.update("counter", |v| {
    ///     let count = v.and_then(|v| v.as_u64()).unwrap_or(0);
    ///     Some(serde_json::json!(count + 1))
    /// });
    /// assert_eq!(ctx.get("counter").and_then(|v| v.as_u64()), Some(1));
    ///
    /// // Conditionally remove a key by returning None
    /// ctx.set("temp", serde_json::json!("remove me"));
    /// ctx.update("temp", |_| None); // Key is now deleted
    /// assert!(!ctx.contains_key("temp"));
    /// ```
    pub fn update<F>(&self, key: &str, f: F)
    where
        F: FnOnce(Option<serde_json::Value>) -> Option<serde_json::Value>,
    {
        match self.store.entry(key.to_owned()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                // Move the value out rather than cloning it: a clone costs time
                // proportional to the value's size, all while this shard's lock
                // is held. If `f` panics the key is left holding `null`.
                let old_value = std::mem::take(entry.get_mut());
                match f(Some(old_value)) {
                    Some(new_value) => {
                        entry.insert(new_value);
                    }
                    None => {
                        entry.remove();
                    }
                }
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                if let Some(new_value) = f(None) {
                    entry.insert(new_value);
                }
            }
        }
    }
}

impl Default for WorkerContext {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::ShutdownSignal;

    #[test]
    fn is_cancelled_tracks_the_request_and_a_vanished_supervisor() {
        let (tx, rx) = tokio::sync::watch::channel(false);
        let signal = ShutdownSignal::new(rx);
        assert!(!signal.is_cancelled());

        tx.send(true).expect("receiver alive");
        assert!(signal.is_cancelled(), "an explicit request is reported");

        let (tx, rx) = tokio::sync::watch::channel(false);
        let orphaned = ShutdownSignal::new(rx);
        drop(tx);
        assert!(
            orphaned.is_cancelled(),
            "with the supervisor side gone, the worker must be told to stop"
        );

        assert!(!ShutdownSignal::never().is_cancelled());
    }
}
