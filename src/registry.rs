//! Finding workers by name, like Erlang's `register` and `whereis`.
//!
//! A worker registers something others can use to reach it, typically its
//! [`MailboxHandle`](crate::MailboxHandle), under a name. Anyone holding the
//! [`Registry`] can then look it up by name and type.
//!
//! Registering returns a [`Registration`] guard; dropping it removes the name.
//! Keep the guard in the worker so the name lives exactly as long as that
//! instance of the worker, as a registered name on the BEAM lives as long as
//! its process:
//!
//! ```
//! use ash_flare::{Registration, Registry, Worker};
//! use ash_flare::mailbox::{mailbox, MailboxConfig, MailboxHandle, Mailbox};
//!
//! struct Echo {
//!     inbox: Mailbox,
//!     handle: MailboxHandle,
//!     registry: Registry,
//!     name: Option<Registration>,
//! }
//!
//! impl Worker for Echo {
//!     type Error = std::io::Error;
//!
//!     async fn initialize(&mut self) -> Result<(), Self::Error> {
//!         self.name = Some(self.registry.register("echo", self.handle.clone()));
//!         Ok(())
//!     }
//!
//!     async fn run(&mut self) -> Result<(), Self::Error> {
//!         while let Some(message) = self.inbox.recv().await {
//!             println!("{message}");
//!         }
//!         Ok(())
//!     }
//! }
//!
//! // Elsewhere:
//! # let registry = Registry::new();
//! if let Some(echo) = registry.whereis::<MailboxHandle>("echo") {
//!     // echo.send("hello").await
//! }
//! ```

use dashmap::DashMap;
use std::any::Any;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

/// A map from names to values of any cloneable type. Cloning a registry is
/// cheap; clones share the same names.
#[derive(Clone, Default)]
pub struct Registry {
    entries: Arc<DashMap<String, Entry>>,
}

struct Entry {
    value: Arc<dyn Any + Send + Sync>,
    /// Identifies the registration that wrote this entry, so a stale guard
    /// never removes a newer registration of the same name.
    token: u64,
}

/// Error from [`Registry::try_register`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RegistryError {
    /// The name is already registered.
    #[error("name '{0}' is already registered")]
    AlreadyRegistered(String),
}

impl Registry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The process-wide registry, for when passing one around is not wanted.
    #[must_use]
    pub fn global() -> &'static Registry {
        static GLOBAL: OnceLock<Registry> = OnceLock::new();
        GLOBAL.get_or_init(Registry::new)
    }

    /// Registers `value` under `name`, replacing any current registration.
    ///
    /// Replacing rather than failing matters across restarts: a restarted
    /// worker may register before its previous instance has been dropped. The
    /// previous instance's guard then leaves the new registration alone.
    #[must_use = "dropping the Registration unregisters the name immediately"]
    pub fn register<T>(&self, name: impl Into<String>, value: T) -> Registration
    where
        T: Clone + Send + Sync + 'static,
    {
        let key = name.into();
        let token = next_token();
        self.entries.insert(
            key.clone(),
            Entry {
                value: Arc::new(value),
                token,
            },
        );
        Registration {
            registry: self.clone(),
            name: key,
            token,
        }
    }

    /// Registers `value` under `name` only if the name is free.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::AlreadyRegistered`] if the name is taken.
    pub fn try_register<T>(
        &self,
        name: impl Into<String>,
        value: T,
    ) -> Result<Registration, RegistryError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let key = name.into();
        match self.entries.entry(key.clone()) {
            dashmap::mapref::entry::Entry::Occupied(_) => {
                Err(RegistryError::AlreadyRegistered(key))
            }
            dashmap::mapref::entry::Entry::Vacant(slot) => {
                let token = next_token();
                slot.insert(Entry {
                    value: Arc::new(value),
                    token,
                });
                Ok(Registration {
                    registry: self.clone(),
                    name: key,
                    token,
                })
            }
        }
    }

    /// Looks up `name`, returning a clone of its value if it is registered
    /// with type `T`.
    #[must_use]
    pub fn whereis<T>(&self, name: &str) -> Option<T>
    where
        T: Clone + 'static,
    {
        self.entries
            .get(name)
            .and_then(|entry| entry.value.downcast_ref::<T>().cloned())
    }

    /// Whether `name` is registered, with any type.
    #[must_use]
    pub fn is_registered(&self, name: &str) -> bool {
        self.entries.contains_key(name)
    }

    /// The currently registered names, in no particular order.
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        self.entries
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Removes `name` regardless of who registered it. Returns whether it was
    /// registered.
    #[allow(clippy::must_use_candidate)]
    pub fn unregister(&self, name: &str) -> bool {
        self.entries.remove(name).is_some()
    }
}

impl fmt::Debug for Registry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registry")
            .field("names", &self.entries.len())
            .finish_non_exhaustive()
    }
}

/// Keeps a name registered; dropping it unregisters the name, unless it has
/// since been registered again by someone else.
pub struct Registration {
    registry: Registry,
    name: String,
    token: u64,
}

impl Registration {
    /// The registered name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.registry
            .entries
            .remove_if(&self.name, |_, entry| entry.token == self.token);
    }
}

impl fmt::Debug for Registration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registration")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

fn next_token() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}
