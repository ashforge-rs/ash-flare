//! Distributed supervision via TCP/Unix sockets
//!
//! Allows supervisors to run in separate processes (local) or on remote machines (network).
//! Commands are serialized with rkyv for minimal overhead.
//!
//! # Security
//!
//! **This protocol has no authentication, authorization, or encryption.** Any
//! peer able to open a connection can enumerate children, terminate an
//! individual child, or shut the whole supervision tree down via
//! [`RemoteCommand::Shutdown`]. Traffic is sent in the clear.
//!
//! Only listen on an interface you fully control. Prefer
//! [`SupervisorServer::listen_unix`] with restrictive filesystem permissions;
//! if you use [`SupervisorServer::listen_tcp`], bind to loopback or place the
//! listener behind an authenticated transport such as a VPN, SSH tunnel, or
//! mutual-TLS proxy. Do not expose it to an untrusted network.
//!
//! Incoming frames are length-bounded and validated before access, so a
//! malformed payload is rejected as an error rather than causing undefined
//! behaviour — but validation is not a substitute for access control.

#![allow(missing_docs)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};

use crate::{ChildInfo as SupervisorChildInfo, ChildType, RestartPolicy, SupervisorHandle, Worker};

/// Maximum accepted frame size (10 MiB).
///
/// The length prefix arrives from an untrusted peer, so it is bounded before
/// allocating to prevent a tiny frame from requesting a huge allocation.
const MAX_MESSAGE_BYTES: usize = 10_000_000;

/// Remote supervisor address
#[derive(Debug, Clone, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)]
#[rkyv(derive(Debug))]
pub enum SupervisorAddress {
    /// TCP socket address (host:port)
    Tcp(String),
    /// Unix domain socket path
    Unix(String),
}

/// Commands that can be sent to a remote supervisor
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)]
#[rkyv(derive(Debug))]
#[rkyv(attr(allow(missing_docs)))]
pub enum RemoteCommand {
    /// Request supervisor to shut down gracefully
    Shutdown,
    /// Get list of all children
    WhichChildren,
    /// Terminate a specific child
    TerminateChild {
        /// ID of the child to terminate
        id: String,
    },
    /// Get supervisor status
    Status,
}

/// Responses from remote supervisor
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)]
#[rkyv(derive(Debug))]
#[rkyv(attr(allow(missing_docs)))]
pub enum RemoteResponse {
    /// Command executed successfully
    Ok,
    /// List of child IDs
    Children(Vec<ChildInfo>),
    /// Supervisor status information
    Status(SupervisorStatus),
    /// Error occurred
    Error(String),
}

/// Information about a child process (simplified for serialization)
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)]
#[rkyv(derive(Debug))]
#[rkyv(attr(allow(missing_docs)))]
pub struct ChildInfo {
    /// Unique identifier for the child
    pub id: String,
    /// Type of child (Worker or Supervisor)
    pub child_type: ChildType,
    /// Restart policy for the child (None for supervisors)
    pub restart_policy: Option<RestartPolicy>,
}

impl From<SupervisorChildInfo> for ChildInfo {
    fn from(info: SupervisorChildInfo) -> Self {
        Self {
            id: info.id,
            child_type: info.child_type,
            restart_policy: info.restart_policy,
        }
    }
}

/// Status information about a supervisor
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)]
#[rkyv(derive(Debug))]
#[rkyv(attr(allow(missing_docs)))]
pub struct SupervisorStatus {
    /// Name of the supervisor
    pub name: String,
    /// Number of children currently running
    pub children_count: usize,
    /// Restart strategy being used
    pub restart_strategy: String,
    /// Uptime in seconds
    pub uptime_secs: u64,
}

/// Handle to communicate with a remote supervisor
#[derive(Clone)]
pub struct RemoteSupervisorHandle {
    address: SupervisorAddress,
}

impl RemoteSupervisorHandle {
    /// Create a new handle to a remote supervisor
    #[must_use]
    pub fn new(address: SupervisorAddress) -> Self {
        Self { address }
    }

    /// Connect to a TCP supervisor
    ///
    /// # Errors
    ///
    /// Returns an error if the address is invalid.
    #[allow(clippy::unused_async)]
    pub async fn connect_tcp(addr: impl Into<String>) -> Result<Self, DistributedError> {
        let address = SupervisorAddress::Tcp(addr.into());
        Ok(Self { address })
    }

    /// Connect to a Unix socket supervisor
    ///
    /// # Errors
    ///
    /// Returns an error if the path is invalid.
    #[allow(clippy::unused_async)]
    pub async fn connect_unix(path: impl Into<String>) -> Result<Self, DistributedError> {
        let address = SupervisorAddress::Unix(path.into());
        Ok(Self { address })
    }

    /// Send a command to the remote supervisor
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails or the command cannot be serialized.
    pub async fn send_command(
        &self,
        cmd: RemoteCommand,
    ) -> Result<RemoteResponse, DistributedError> {
        match &self.address {
            SupervisorAddress::Tcp(addr) => {
                let mut stream = TcpStream::connect(addr).await?;
                send_message(&mut stream, &cmd).await?;
                receive_message(&mut stream).await
            }
            #[cfg(unix)]
            SupervisorAddress::Unix(path) => {
                let mut stream = UnixStream::connect(path).await?;
                send_message(&mut stream, &cmd).await?;
                receive_message(&mut stream).await
            }
            #[cfg(not(unix))]
            SupervisorAddress::Unix(_) => Err(DistributedError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Unix sockets are not supported on this platform",
            ))),
        }
    }

    /// Shutdown the remote supervisor
    ///
    /// # Errors
    ///
    /// Returns an error if the remote connection fails.
    pub async fn shutdown(&self) -> Result<(), DistributedError> {
        self.send_command(RemoteCommand::Shutdown).await?;
        Ok(())
    }

    /// Get list of children from remote supervisor
    ///
    /// # Errors
    ///
    /// Returns an error if the remote connection fails or returns an unexpected response.
    pub async fn which_children(&self) -> Result<Vec<ChildInfo>, DistributedError> {
        match self.send_command(RemoteCommand::WhichChildren).await? {
            RemoteResponse::Children(children) => Ok(children),
            RemoteResponse::Error(e) => Err(DistributedError::RemoteError(e)),
            _ => Err(DistributedError::UnexpectedResponse),
        }
    }

    /// Terminate a child on the remote supervisor
    ///
    /// # Errors
    ///
    /// Returns an error if the remote connection fails or returns an unexpected response.
    pub async fn terminate_child(&self, id: &str) -> Result<(), DistributedError> {
        match self
            .send_command(RemoteCommand::TerminateChild { id: id.to_owned() })
            .await?
        {
            RemoteResponse::Ok => Ok(()),
            RemoteResponse::Error(e) => Err(DistributedError::RemoteError(e)),
            _ => Err(DistributedError::UnexpectedResponse),
        }
    }

    /// Get status from remote supervisor
    ///
    /// # Errors
    ///
    /// Returns an error if the remote connection fails or returns an unexpected response.
    pub async fn status(&self) -> Result<SupervisorStatus, DistributedError> {
        match self.send_command(RemoteCommand::Status).await? {
            RemoteResponse::Status(status) => Ok(status),
            RemoteResponse::Error(e) => Err(DistributedError::RemoteError(e)),
            _ => Err(DistributedError::UnexpectedResponse),
        }
    }
}

/// Server that wraps a `SupervisorHandle` and accepts remote commands
pub struct SupervisorServer<W: Worker> {
    handle: Arc<SupervisorHandle<W>>,
}

impl<W: Worker> SupervisorServer<W> {
    /// Create a new supervisor server wrapping a `SupervisorHandle`
    #[must_use]
    pub fn new(handle: SupervisorHandle<W>) -> Self {
        Self {
            handle: Arc::new(handle),
        }
    }

    /// Start listening on a Unix socket (Unix only)
    ///
    /// # Security
    ///
    /// Connections are **not authenticated**. Access is governed solely by the
    /// filesystem permissions on `path`; restrict them to the intended user.
    /// See the [module documentation](self#security).
    ///
    /// # Errors
    ///
    /// Returns an error if the socket cannot be bound or a connection fails.
    #[cfg(unix)]
    pub async fn listen_unix(
        self,
        path: impl AsRef<std::path::Path>,
    ) -> Result<(), DistributedError> {
        let socket_path = path.as_ref();
        let _remove_result = std::fs::remove_file(socket_path); // Clean up old socket

        let listener = UnixListener::bind(socket_path)?;
        tracing::info!(path = %socket_path.display(), "server listening on unix socket");

        loop {
            let (mut stream, _) = listener.accept().await?;
            let handle = Arc::clone(&self.handle);

            tokio::spawn(async move {
                if let Err(e) = Self::handle_connection(&mut stream, handle).await {
                    tracing::error!(error = %e, "connection error");
                }
            });
        }
    }

    /// Start listening on a TCP socket
    ///
    /// # Security
    ///
    /// Connections are **not authenticated or encrypted**. Any peer that can
    /// reach `addr` may terminate children or shut down the supervision tree.
    /// Bind to loopback, or front this with an authenticated transport. See the
    /// [module documentation](self#security).
    ///
    /// # Errors
    ///
    /// Returns an error if the socket cannot be bound or a connection fails.
    pub async fn listen_tcp(self, addr: impl AsRef<str>) -> Result<(), DistributedError> {
        let listener = TcpListener::bind(addr.as_ref()).await?;
        tracing::info!(address = addr.as_ref(), "server listening on tcp");

        loop {
            let (mut stream, peer) = listener.accept().await?;
            tracing::debug!(peer = ?peer, "new connection");
            let handle = Arc::clone(&self.handle);

            tokio::spawn(async move {
                if let Err(e) = Self::handle_connection(&mut stream, handle).await {
                    tracing::error!(error = %e, "Connection error");
                }
            });
        }
    }

    async fn handle_connection<S>(
        stream: &mut S,
        handle: Arc<SupervisorHandle<W>>,
    ) -> Result<(), DistributedError>
    where
        S: AsyncReadExt + AsyncWriteExt + Unpin,
    {
        let command: RemoteCommand = receive_message(stream).await?;
        let response = Self::process_command(command, &handle).await;
        send_message(stream, &response).await?;
        Ok(())
    }

    async fn process_command(
        command: RemoteCommand,
        handle: &SupervisorHandle<W>,
    ) -> RemoteResponse {
        match command {
            RemoteCommand::Shutdown => match handle.shutdown().await {
                Ok(()) => RemoteResponse::Ok,
                Err(e) => RemoteResponse::Error(e.to_string()),
            },
            RemoteCommand::WhichChildren => match handle.which_children().await {
                Ok(children) => {
                    let child_list: Vec<ChildInfo> = children.into_iter().map(Into::into).collect();
                    RemoteResponse::Children(child_list)
                }
                Err(e) => RemoteResponse::Error(e.to_string()),
            },
            RemoteCommand::TerminateChild { id } => match handle.terminate_child(&id).await {
                Ok(()) => RemoteResponse::Ok,
                Err(e) => RemoteResponse::Error(e.to_string()),
            },
            RemoteCommand::Status => {
                let restart_strategy = handle
                    .restart_strategy()
                    .await
                    .map_or_else(|_| "Unknown".to_owned(), |s| format!("{s:?}"));
                let uptime_secs = handle.uptime().await.unwrap_or(0);

                RemoteResponse::Status(SupervisorStatus {
                    name: handle.name().to_owned(),
                    children_count: handle.which_children().await.map_or(0, |c| c.len()),
                    restart_strategy,
                    uptime_secs,
                })
            }
        }
    }
}

/// Send a message over a stream (length-prefixed rkyv)
async fn send_message<S, T>(stream: &mut S, msg: &T) -> Result<(), DistributedError>
where
    S: AsyncWriteExt + Unpin,
    T: Serialize,
    for<'a> T: RkyvSerialize<
        rkyv::api::high::HighSerializer<
            rkyv::util::AlignedVec,
            rkyv::ser::allocator::ArenaHandle<'a>,
            rkyv::rancor::Error,
        >,
    >,
{
    let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(msg).map_err(DistributedError::Encode)?;
    if encoded.len() > MAX_MESSAGE_BYTES {
        return Err(DistributedError::MessageTooLarge(encoded.len()));
    }
    let len = u32::try_from(encoded.len())
        .map_err(|_| DistributedError::MessageTooLarge(encoded.len()))?;

    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(&encoded).await?;
    stream.flush().await?;

    Ok(())
}

/// Receive a message from a stream (length-prefixed rkyv)
///
/// The payload arrives from a socket and is therefore untrusted: the archive is
/// validated with bytecheck before access, so a malformed or hostile frame
/// surfaces as [`DistributedError::Decode`] rather than undefined behaviour.
async fn receive_message<S, T>(stream: &mut S) -> Result<T, DistributedError>
where
    S: AsyncReadExt + Unpin,
    T: Archive,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
    T::Archived: RkyvDeserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>,
{
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes).await?;
    let len = usize::try_from(u32::from_be_bytes(len_bytes))
        .map_err(|_| DistributedError::MessageTooLarge(usize::MAX))?;

    if len > MAX_MESSAGE_BYTES {
        return Err(DistributedError::MessageTooLarge(len));
    }

    let mut buffer = vec![0u8; len];
    stream.read_exact(&mut buffer).await?;

    rkyv::from_bytes::<T, rkyv::rancor::Error>(&buffer).map_err(DistributedError::Decode)
}

/// Errors that can occur in distributed operations
#[derive(Debug)]
pub enum DistributedError {
    /// I/O error occurred
    Io(std::io::Error),
    /// Serialization error
    Encode(rkyv::rancor::Error),
    /// Deserialization error
    Decode(rkyv::rancor::Error),
    /// Error from remote supervisor
    RemoteError(String),
    /// Received unexpected response type
    UnexpectedResponse,
    /// Message size exceeds maximum allowed
    MessageTooLarge(usize),
}

impl fmt::Display for DistributedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DistributedError::Io(e) => write!(f, "IO error: {e}"),
            DistributedError::Encode(e) => write!(f, "Encode error: {e}"),
            DistributedError::Decode(e) => write!(f, "Decode error: {e}"),
            DistributedError::RemoteError(e) => write!(f, "Remote error: {e}"),
            DistributedError::UnexpectedResponse => write!(f, "Unexpected response from remote"),
            DistributedError::MessageTooLarge(size) => {
                write!(f, "Message too large: {size} bytes")
            }
        }
    }
}

impl std::error::Error for DistributedError {}

impl From<std::io::Error> for DistributedError {
    fn from(e: std::io::Error) -> Self {
        DistributedError::Io(e)
    }
}

// No blanket `From<rkyv::rancor::Error>`: encode and decode failures are
// mapped explicitly at each call site so a decode failure is never reported
// as an encode failure.
