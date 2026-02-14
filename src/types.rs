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
pub enum ChildExitReason {
    /// Normal termination
    Normal,
    /// Abnormal termination (error or panic)
    Abnormal,
    /// Shutdown requested
    Shutdown,
}

/// Child identifier type
pub type ChildId = String;

/// Information about a child process
#[derive(Debug, Clone)]
pub struct ChildInfo {
    /// Unique identifier for the child
    pub id: ChildId,
    /// Type of child (Worker or Supervisor)
    pub child_type: ChildType,
    /// Restart policy for the child (None for supervisors)
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

/// Shared context for stateful workers with in-memory key-value store.
///
/// Provides a process-local, concurrency-safe storage for workers to share state.
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
    /// Creates a new empty WorkerContext.
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
    pub fn delete(&self, key: &str) -> Option<serde_json::Value> {
        self.store.remove(key).map(|(_, v)| v)
    }

    /// Returns the number of key-value pairs in the store.
    #[inline]
    pub fn len(&self) -> usize {
        self.store.len()
    }

    /// Returns `true` if the store contains no key-value pairs.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }

    /// Checks if a key exists in the store without retrieving the value.
    #[inline]
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
                let old_value = entry.get().clone();
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

// ============================================================================
// Beam-ready Foundation Types
// ============================================================================

/// Process identifier (Beam-style) - foundation for future linking/monitoring
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Pid(pub u64);

/// Extended exit reason for future Beam-like semantics
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum ExitReason {
    /// Normal termination
    Normal,
    /// Killed/aborted
    Killed,
    /// Terminated with error
    Error(String),
}
