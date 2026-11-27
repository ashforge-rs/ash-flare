//! Typed mailboxes for message-passing workers.
//!
//! A mailbox is a queue with any number of senders ([`MailboxHandle`]) and a
//! receiving end ([`Mailbox`]). Messages are `String`s unless another type is
//! chosen, typically an enum describing everything a worker understands.
//!
//! # Surviving restarts
//!
//! Cloning a [`Mailbox`] shares its queue. Capture one in a worker's factory
//! and hand each new instance a clone: senders keep working across restarts,
//! and messages sent while the worker was restarting are waiting for its
//! replacement. Several workers holding clones of one mailbox form a pool,
//! each message going to whichever is free.
//!
//! # Requests and replies
//!
//! [`MailboxHandle::send`] is a one-way message (OTP's `cast`).
//! [`MailboxHandle::call`] sends a message carrying a [`Reply`] and waits for
//! the answer (OTP's `call`):
//!
//! ```
//! use ash_flare::mailbox::{mailbox, MailboxConfig, Reply};
//! use std::time::Duration;
//!
//! enum Counter {
//!     Add(u64),
//!     Get(Reply<u64>),
//! }
//!
//! # #[tokio::main]
//! # async fn main() {
//! let (handle, inbox) = mailbox::<Counter>(MailboxConfig::default());
//!
//! tokio::spawn(async move {
//!     let mut total = 0;
//!     while let Some(message) = inbox.recv().await {
//!         match message {
//!             Counter::Add(n) => total += n,
//!             Counter::Get(reply) => reply.send(total),
//!         }
//!     }
//! });
//!
//! handle.send(Counter::Add(2)).await.unwrap();
//! let total = handle.call(Counter::Get, Duration::from_secs(1)).await.unwrap();
//! assert_eq!(total, 2);
//! # }
//! ```

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, mpsc, oneshot};

/// Configuration for mailbox capacity
#[derive(Debug, Clone, Copy)]
pub enum MailboxConfig {
    /// Unbounded mailbox - no capacity limit
    Unbounded,
    /// Bounded mailbox with fixed capacity
    Bounded {
        /// Maximum number of messages the mailbox can hold
        capacity: usize,
    },
}

impl Default for MailboxConfig {
    fn default() -> Self {
        Self::Bounded { capacity: 100 }
    }
}

impl MailboxConfig {
    /// Create unbounded mailbox configuration
    #[must_use]
    pub const fn unbounded() -> Self {
        Self::Unbounded
    }

    /// Create bounded mailbox configuration
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is 0.
    #[must_use]
    pub fn bounded(capacity: usize) -> Self {
        assert!(capacity > 0, "mailbox capacity must be > 0");
        Self::Bounded { capacity }
    }
}

/// Sending end of a mailbox. Cheap to clone.
pub struct MailboxHandle<M = String> {
    tx: MailboxSender<M>,
    worker_id: Arc<str>,
}

impl<M> Clone for MailboxHandle<M> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            worker_id: Arc::clone(&self.worker_id),
        }
    }
}

impl<M> std::fmt::Debug for MailboxHandle<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MailboxHandle")
            .field("worker_id", &self.worker_id)
            .finish_non_exhaustive()
    }
}

enum MailboxSender<M> {
    Bounded(mpsc::Sender<M>),
    Unbounded(mpsc::UnboundedSender<M>),
}

impl<M> Clone for MailboxSender<M> {
    fn clone(&self) -> Self {
        match self {
            Self::Bounded(tx) => Self::Bounded(tx.clone()),
            Self::Unbounded(tx) => Self::Unbounded(tx.clone()),
        }
    }
}

impl<M> MailboxHandle<M> {
    /// Sends a message, waiting for room if the mailbox is bounded and full.
    ///
    /// # Errors
    ///
    /// Returns `SendError::Closed` if every [`Mailbox`] has been dropped.
    pub async fn send(&self, message: impl Into<M>) -> Result<(), SendError> {
        let msg = message.into();
        match &self.tx {
            MailboxSender::Bounded(tx) => tx.send(msg).await.map_err(|_| self.closed()),
            MailboxSender::Unbounded(tx) => tx.send(msg).map_err(|_| self.closed()),
        }
    }

    /// Sends a message without waiting.
    ///
    /// # Errors
    ///
    /// Returns `TrySendError::Full` if the mailbox is full or `TrySendError::Closed` if every [`Mailbox`] has been dropped.
    pub fn try_send(&self, message: impl Into<M>) -> Result<(), TrySendError> {
        let msg = message.into();
        match &self.tx {
            MailboxSender::Bounded(tx) => tx.try_send(msg).map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => TrySendError::Full,
                mpsc::error::TrySendError::Closed(_) => {
                    TrySendError::Closed(self.worker_id.to_string())
                }
            }),
            MailboxSender::Unbounded(tx) => tx
                .send(msg)
                .map_err(|_| TrySendError::Closed(self.worker_id.to_string())),
        }
    }

    /// Sends a request and waits for its reply, like OTP's `gen_server:call`.
    ///
    /// `request` builds the message from the [`Reply`] the receiver answers
    /// through; for an enum variant holding only the `Reply`, the variant
    /// itself can be passed.
    ///
    /// # Errors
    ///
    /// - [`CallError::Closed`] if every [`Mailbox`] has been dropped;
    /// - [`CallError::Timeout`] if no reply arrived within `timeout`;
    /// - [`CallError::NoReply`] if the receiver dropped the [`Reply`] without
    ///   answering, for example because the worker crashed handling it.
    pub async fn call<R>(
        &self,
        request: impl FnOnce(Reply<R>) -> M,
        timeout: Duration,
    ) -> Result<R, CallError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.send(request(Reply(reply_tx)))
            .await
            .map_err(|SendError::Closed(id)| CallError::Closed(id))?;

        match tokio::time::timeout(timeout, reply_rx).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(_dropped)) => Err(CallError::NoReply),
            Err(_elapsed) => Err(CallError::Timeout(timeout)),
        }
    }

    /// Get the worker ID
    #[must_use]
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Check if the mailbox is still open
    #[must_use]
    pub fn is_open(&self) -> bool {
        match &self.tx {
            MailboxSender::Bounded(tx) => !tx.is_closed(),
            MailboxSender::Unbounded(tx) => !tx.is_closed(),
        }
    }

    fn closed(&self) -> SendError {
        SendError::Closed(self.worker_id.to_string())
    }
}

/// The receiving end of a [`MailboxHandle::call`]: answer the caller with
/// [`Reply::send`]. Dropping it unanswered makes the call fail with
/// [`CallError::NoReply`].
#[derive(Debug)]
pub struct Reply<R>(oneshot::Sender<R>);

impl<R> Reply<R> {
    /// Answers the caller. Does nothing if the caller has stopped waiting.
    pub fn send(self, value: R) {
        let _delivered = self.0.send(value);
    }

    /// Whether the caller is still waiting for the answer.
    #[must_use]
    pub fn is_wanted(&self) -> bool {
        !self.0.is_closed()
    }
}

/// Receiving end of a mailbox.
///
/// Clones share one queue: hand a clone to each instance a worker's factory
/// builds and the mailbox outlives restarts, or to several workers to form a
/// pool. Each message is received by exactly one of them.
pub struct Mailbox<M = String> {
    rx: Arc<Mutex<MailboxReceiver<M>>>,
}

impl<M> Clone for Mailbox<M> {
    fn clone(&self) -> Self {
        Self {
            rx: Arc::clone(&self.rx),
        }
    }
}

impl<M> std::fmt::Debug for Mailbox<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mailbox").finish_non_exhaustive()
    }
}

enum MailboxReceiver<M> {
    Bounded(mpsc::Receiver<M>),
    Unbounded(mpsc::UnboundedReceiver<M>),
}

impl<M> Mailbox<M> {
    fn new(rx: MailboxReceiver<M>) -> Self {
        Self {
            rx: Arc::new(Mutex::new(rx)),
        }
    }

    /// Receives the next message, waiting until one arrives. Returns `None`
    /// once every [`MailboxHandle`] has been dropped and the queue is empty.
    ///
    /// Cancel-safe: if the returned future is dropped (for example when the
    /// worker is stopped), no message is lost.
    pub async fn recv(&self) -> Option<M> {
        let mut receiver = self.rx.lock().await;
        match &mut *receiver {
            MailboxReceiver::Bounded(rx) => rx.recv().await,
            MailboxReceiver::Unbounded(rx) => rx.recv().await,
        }
    }

    /// Receives a message if one is ready.
    ///
    /// # Errors
    ///
    /// Returns `TryRecvError::Empty` if no message is ready (including while
    /// another clone is receiving) or `TryRecvError::Disconnected` if every
    /// [`MailboxHandle`] has been dropped.
    pub fn try_recv(&self) -> Result<M, TryRecvError> {
        let Ok(mut receiver) = self.rx.try_lock() else {
            return Err(TryRecvError::Empty);
        };
        let result = match &mut *receiver {
            MailboxReceiver::Bounded(rx) => rx.try_recv(),
            MailboxReceiver::Unbounded(rx) => rx.try_recv(),
        };
        result.map_err(|e| match e {
            mpsc::error::TryRecvError::Empty => TryRecvError::Empty,
            mpsc::error::TryRecvError::Disconnected => TryRecvError::Disconnected,
        })
    }
}

/// Creates a mailbox.
#[must_use]
pub fn mailbox<M>(config: MailboxConfig) -> (MailboxHandle<M>, Mailbox<M>) {
    mailbox_named(config, "unnamed")
}

/// Creates a mailbox whose errors name `worker_id`.
pub fn mailbox_named<M>(
    config: MailboxConfig,
    worker_id: impl Into<String>,
) -> (MailboxHandle<M>, Mailbox<M>) {
    let id: Arc<str> = Arc::from(worker_id.into());
    let (tx, rx) = match config {
        MailboxConfig::Unbounded => {
            let (tx, rx) = mpsc::unbounded_channel();
            (MailboxSender::Unbounded(tx), MailboxReceiver::Unbounded(rx))
        }
        MailboxConfig::Bounded { capacity } => {
            let (tx, rx) = mpsc::channel(capacity);
            (MailboxSender::Bounded(tx), MailboxReceiver::Bounded(rx))
        }
    };
    (MailboxHandle { tx, worker_id: id }, Mailbox::new(rx))
}

/// Error when sending a message fails
#[derive(Debug, Clone, thiserror::Error)]
pub enum SendError {
    /// The worker's mailbox is closed
    #[error("worker '{0}' mailbox is closed")]
    Closed(String),
}

/// Error when trying to send without blocking
#[derive(Debug, Clone, thiserror::Error)]
pub enum TrySendError {
    /// The mailbox is full
    #[error("mailbox is full")]
    Full,
    /// The worker's mailbox is closed
    #[error("worker '{0}' mailbox is closed")]
    Closed(String),
}

/// Error when trying to receive without blocking
#[derive(Debug, Clone, thiserror::Error)]
pub enum TryRecvError {
    /// The mailbox is empty
    #[error("mailbox is empty")]
    Empty,
    /// The mailbox is disconnected
    #[error("mailbox is disconnected")]
    Disconnected,
}

/// Error from [`MailboxHandle::call`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum CallError {
    /// The worker's mailbox is closed.
    #[error("worker '{0}' mailbox is closed")]
    Closed(String),
    /// No reply arrived in time.
    #[error("no reply within {0:?}")]
    Timeout(Duration),
    /// The receiver dropped the request without replying, for example because
    /// the worker crashed while handling it.
    #[error("the request was dropped without a reply")]
    NoReply,
}
