//! Optional mailbox system for workers
//!
//! This module provides simple message-passing capabilities for workers,
//! allowing them to receive string messages via a mailbox.

use tokio::sync::mpsc;

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
    pub const fn unbounded() -> Self {
        Self::Unbounded
    }

    /// Create bounded mailbox configuration
    pub fn bounded(capacity: usize) -> Self {
        assert!(capacity > 0, "mailbox capacity must be > 0");
        Self::Bounded { capacity }
    }
}

/// Handle for sending messages to a mailbox
#[derive(Clone)]
pub struct MailboxHandle {
    tx: MailboxSender,
    worker_id: String,
}

enum MailboxSender {
    Bounded(mpsc::Sender<String>),
    Unbounded(mpsc::UnboundedSender<String>),
}

impl Clone for MailboxSender {
    fn clone(&self) -> Self {
        match self {
            Self::Bounded(tx) => Self::Bounded(tx.clone()),
            Self::Unbounded(tx) => Self::Unbounded(tx.clone()),
        }
    }
}

impl MailboxHandle {
    pub(crate) fn new_bounded(tx: mpsc::Sender<String>, worker_id: impl Into<String>) -> Self {
        Self {
            tx: MailboxSender::Bounded(tx),
            worker_id: worker_id.into(),
        }
    }

    pub(crate) fn new_unbounded(
        tx: mpsc::UnboundedSender<String>,
        worker_id: impl Into<String>,
    ) -> Self {
        Self {
            tx: MailboxSender::Unbounded(tx),
            worker_id: worker_id.into(),
        }
    }

    /// Send a message to the worker (async, waits if mailbox full)
    pub async fn send(&self, msg: impl Into<String>) -> Result<(), SendError> {
        let msg = msg.into();
        match &self.tx {
            MailboxSender::Bounded(tx) => tx
                .send(msg)
                .await
                .map_err(|_| SendError::Closed(self.worker_id.clone())),
            MailboxSender::Unbounded(tx) => tx
                .send(msg)
                .map_err(|_| SendError::Closed(self.worker_id.clone())),
        }
    }

    /// Try to send a message without blocking
    pub fn try_send(&self, msg: impl Into<String>) -> Result<(), TrySendError> {
        let msg = msg.into();
        match &self.tx {
            MailboxSender::Bounded(tx) => tx.try_send(msg).map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => TrySendError::Full,
                mpsc::error::TrySendError::Closed(_) => {
                    TrySendError::Closed(self.worker_id.clone())
                }
            }),
            MailboxSender::Unbounded(tx) => tx
                .send(msg)
                .map_err(|_| TrySendError::Closed(self.worker_id.clone())),
        }
    }

    /// Get the worker ID
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Check if the mailbox is still open
    pub fn is_open(&self) -> bool {
        match &self.tx {
            MailboxSender::Bounded(tx) => !tx.is_closed(),
            MailboxSender::Unbounded(tx) => !tx.is_closed(),
        }
    }
}

/// Mailbox for receiving messages in a worker
pub struct Mailbox {
    rx: MailboxReceiver,
}

enum MailboxReceiver {
    Bounded(mpsc::Receiver<String>),
    Unbounded(mpsc::UnboundedReceiver<String>),
}

impl Mailbox {
    pub(crate) fn new_bounded(rx: mpsc::Receiver<String>) -> Self {
        Self {
            rx: MailboxReceiver::Bounded(rx),
        }
    }

    pub(crate) fn new_unbounded(rx: mpsc::UnboundedReceiver<String>) -> Self {
        Self {
            rx: MailboxReceiver::Unbounded(rx),
        }
    }

    /// Receive a message from the mailbox
    pub async fn recv(&mut self) -> Option<String> {
        match &mut self.rx {
            MailboxReceiver::Bounded(rx) => rx.recv().await,
            MailboxReceiver::Unbounded(rx) => rx.recv().await,
        }
    }

    /// Try to receive a message without blocking
    pub fn try_recv(&mut self) -> Result<String, TryRecvError> {
        match &mut self.rx {
            MailboxReceiver::Bounded(rx) => rx.try_recv().map_err(|e| match e {
                mpsc::error::TryRecvError::Empty => TryRecvError::Empty,
                mpsc::error::TryRecvError::Disconnected => TryRecvError::Disconnected,
            }),
            MailboxReceiver::Unbounded(rx) => rx.try_recv().map_err(|e| match e {
                mpsc::error::TryRecvError::Empty => TryRecvError::Empty,
                mpsc::error::TryRecvError::Disconnected => TryRecvError::Disconnected,
            }),
        }
    }
}

/// Create a mailbox channel
pub fn mailbox(config: MailboxConfig) -> (MailboxHandle, Mailbox) {
    match config {
        MailboxConfig::Unbounded => {
            let (tx, rx) = mpsc::unbounded_channel();
            (
                MailboxHandle::new_unbounded(tx, "unnamed"),
                Mailbox::new_unbounded(rx),
            )
        }
        MailboxConfig::Bounded { capacity } => {
            let (tx, rx) = mpsc::channel(capacity);
            (
                MailboxHandle::new_bounded(tx, "unnamed"),
                Mailbox::new_bounded(rx),
            )
        }
    }
}

/// Create a mailbox channel with named worker
pub fn mailbox_named(
    config: MailboxConfig,
    worker_id: impl Into<String>,
) -> (MailboxHandle, Mailbox) {
    let worker_id = worker_id.into();
    match config {
        MailboxConfig::Unbounded => {
            let (tx, rx) = mpsc::unbounded_channel();
            (
                MailboxHandle::new_unbounded(tx, worker_id),
                Mailbox::new_unbounded(rx),
            )
        }
        MailboxConfig::Bounded { capacity } => {
            let (tx, rx) = mpsc::channel(capacity);
            (
                MailboxHandle::new_bounded(tx, worker_id),
                Mailbox::new_bounded(rx),
            )
        }
    }
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
