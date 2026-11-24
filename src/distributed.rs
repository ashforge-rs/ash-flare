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

// The `Archived*` types rkyv generates for the wire format carry no docs of
// their own; every hand-written item here is documented.
#![allow(missing_docs)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};

use crate::{ChildInfo, RestartStrategy, SupervisorHandle};

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

/// Status information about a supervisor
#[derive(Debug, Clone, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)]
#[rkyv(derive(Debug))]
#[rkyv(attr(allow(missing_docs)))]
pub struct SupervisorStatus {
    /// Name of the supervisor
    pub name: String,
    /// Number of children currently running
    pub children_count: usize,
    /// Restart strategy being used
    pub restart_strategy: RestartStrategy,
    /// Uptime in seconds
    pub uptime_secs: u64,
}

/// Handle to communicate with a remote supervisor.
///
/// The handle opens a connection on its first command and reuses it for the
/// ones after, so repeated commands do not pay for a new connection each time.
/// Commands on one handle run one at a time; each clone gets its own
/// connection, so give concurrent tasks their own clone.
///
/// If a reused connection turns out to be dead (for example because the
/// server restarted), the command is retried once on a fresh connection. That
/// only happens when the failure came before any part of the reply arrived.
pub struct RemoteSupervisorHandle {
    address: SupervisorAddress,
    connection: tokio::sync::Mutex<Option<Connection>>,
}

impl Clone for RemoteSupervisorHandle {
    /// The clone talks to the same address over its own connection.
    fn clone(&self) -> Self {
        Self::new(self.address.clone())
    }
}

impl fmt::Debug for RemoteSupervisorHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteSupervisorHandle")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

impl RemoteSupervisorHandle {
    /// Creates a handle to a remote supervisor.
    ///
    /// No connection is made here; the first command opens one, so an
    /// unreachable address surfaces as an error from that command.
    #[must_use]
    pub fn new(address: SupervisorAddress) -> Self {
        Self {
            address,
            connection: tokio::sync::Mutex::new(None),
        }
    }

    /// Creates a handle to a supervisor listening on TCP `addr` (`host:port`).
    #[must_use]
    pub fn tcp(addr: impl Into<String>) -> Self {
        Self::new(SupervisorAddress::Tcp(addr.into()))
    }

    /// Creates a handle to a supervisor listening on the Unix socket at `path`.
    #[must_use]
    pub fn unix(path: impl Into<String>) -> Self {
        Self::new(SupervisorAddress::Unix(path.into()))
    }

    /// The address commands are sent to.
    #[must_use]
    pub fn address(&self) -> &SupervisorAddress {
        &self.address
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
        let mut slot = self.connection.lock().await;

        if let Some(connection) = slot.as_mut() {
            match connection.round_trip(&cmd).await {
                Ok(response) => return Ok(response),
                // The reused connection was already dead and the server never
                // started replying, so sending again cannot run it twice.
                Err(RoundTripError::BeforeReply(_)) => *slot = None,
                Err(RoundTripError::Failed(error)) => {
                    *slot = None;
                    return Err(error);
                }
            }
        }

        let mut connection = Connection::open(&self.address).await?;
        let response = connection
            .round_trip(&cmd)
            .await
            .map_err(RoundTripError::into_inner)?;
        *slot = Some(connection);
        Ok(response)
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
pub struct SupervisorServer {
    handle: Arc<SupervisorHandle>,
}

impl SupervisorServer {
    /// Create a new supervisor server wrapping a `SupervisorHandle`
    #[must_use]
    pub fn new(handle: SupervisorHandle) -> Self {
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
            // Replies are written in one go; do not hold them back waiting
            // for an ACK (Nagle), which stalls request/response traffic.
            let _nodelay = stream.set_nodelay(true);
            let handle = Arc::clone(&self.handle);

            tokio::spawn(async move {
                if let Err(e) = Self::handle_connection(&mut stream, handle).await {
                    tracing::error!(error = %e, "Connection error");
                }
            });
        }
    }

    /// Serves commands on one connection until the client closes it.
    async fn handle_connection<S>(
        stream: &mut S,
        handle: Arc<SupervisorHandle>,
    ) -> Result<(), DistributedError>
    where
        S: AsyncReadExt + AsyncWriteExt + Unpin,
    {
        loop {
            let command: RemoteCommand = match receive_message(stream).await {
                Ok(command) => command,
                // The client closed the connection between commands.
                Err(FrameError::NotStarted(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(());
                }
                Err(e) => return Err(e.into_inner()),
            };
            let response = Self::process_command(command, &handle).await;
            send_message(stream, &response).await?;
        }
    }

    async fn process_command(command: RemoteCommand, handle: &SupervisorHandle) -> RemoteResponse {
        match command {
            RemoteCommand::Shutdown => match handle.shutdown().await {
                Ok(()) => RemoteResponse::Ok,
                Err(e) => RemoteResponse::Error(e.to_string()),
            },
            RemoteCommand::WhichChildren => match handle.which_children().await {
                Ok(children) => RemoteResponse::Children(children),
                Err(e) => RemoteResponse::Error(e.to_string()),
            },
            RemoteCommand::TerminateChild { id } => match handle.terminate_child(&id).await {
                Ok(()) => RemoteResponse::Ok,
                Err(e) => RemoteResponse::Error(e.to_string()),
            },
            RemoteCommand::Status => {
                // One request to the supervisor rather than three, which also
                // makes the fields consistent with each other.
                match handle.status_snapshot().await {
                    Ok(snapshot) => RemoteResponse::Status(SupervisorStatus {
                        name: handle.name().to_owned(),
                        children_count: snapshot.children_count,
                        restart_strategy: snapshot.restart_strategy,
                        uptime_secs: snapshot.uptime.as_secs(),
                    }),
                    Err(e) => RemoteResponse::Error(e.to_string()),
                }
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

    // One write per frame: a separate small write for the length prefix can
    // be held back by Nagle's algorithm until the peer ACKs it.
    let mut frame = Vec::with_capacity(encoded.len().saturating_add(4));
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&encoded);
    stream.write_all(&frame).await?;
    stream.flush().await?;

    Ok(())
}

/// Receive a message from a stream (length-prefixed rkyv)
///
/// The payload arrives from a socket and is therefore untrusted: the archive is
/// validated with bytecheck before access, so a malformed or hostile frame
/// surfaces as [`DistributedError::Decode`] rather than undefined behaviour.
async fn receive_message<S, T>(stream: &mut S) -> Result<T, FrameError>
where
    S: AsyncReadExt + Unpin,
    T: Archive,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
    T::Archived: RkyvDeserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>,
{
    let mut len_bytes = [0u8; 4];
    // The first read tells "nothing arrived" apart from "the frame broke off".
    let first = stream
        .read(&mut len_bytes)
        .await
        .map_err(FrameError::NotStarted)?;
    if first == 0 {
        return Err(FrameError::NotStarted(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "connection closed",
        )));
    }

    let failed = |e: std::io::Error| FrameError::Failed(DistributedError::Io(e));
    stream
        .read_exact(len_bytes.get_mut(first..).unwrap_or_default())
        .await
        .map_err(failed)?;
    let len = usize::try_from(u32::from_be_bytes(len_bytes))
        .map_err(|_| FrameError::Failed(DistributedError::MessageTooLarge(usize::MAX)))?;

    if len > MAX_MESSAGE_BYTES {
        return Err(FrameError::Failed(DistributedError::MessageTooLarge(len)));
    }

    let mut buffer = vec![0u8; len];
    stream.read_exact(&mut buffer).await.map_err(failed)?;

    rkyv::from_bytes::<T, rkyv::rancor::Error>(&buffer)
        .map_err(|e| FrameError::Failed(DistributedError::Decode(e)))
}

/// Why reading a frame failed.
enum FrameError {
    /// The connection closed or failed before any byte of the frame arrived.
    NotStarted(std::io::Error),
    /// The frame broke off part-way or could not be decoded.
    Failed(DistributedError),
}

impl FrameError {
    fn into_inner(self) -> DistributedError {
        match self {
            FrameError::NotStarted(e) => DistributedError::Io(e),
            FrameError::Failed(e) => e,
        }
    }
}

/// Why a request/response exchange failed.
enum RoundTripError {
    /// Nothing of the reply arrived, so the server cannot have answered.
    BeforeReply(DistributedError),
    /// The exchange failed after the server may have acted on the command.
    Failed(DistributedError),
}

impl RoundTripError {
    fn into_inner(self) -> DistributedError {
        match self {
            RoundTripError::BeforeReply(e) | RoundTripError::Failed(e) => e,
        }
    }
}

/// An open connection to a remote supervisor.
enum Connection {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(UnixStream),
}

impl Connection {
    async fn open(address: &SupervisorAddress) -> Result<Self, DistributedError> {
        match address {
            SupervisorAddress::Tcp(addr) => {
                let stream = TcpStream::connect(addr).await?;
                // Requests are written in one go; do not hold them back
                // waiting for an ACK.
                stream.set_nodelay(true)?;
                Ok(Connection::Tcp(stream))
            }
            #[cfg(unix)]
            SupervisorAddress::Unix(path) => Ok(Connection::Unix(UnixStream::connect(path).await?)),
            #[cfg(not(unix))]
            SupervisorAddress::Unix(_) => Err(DistributedError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Unix sockets are not supported on this platform",
            ))),
        }
    }

    async fn round_trip(&mut self, cmd: &RemoteCommand) -> Result<RemoteResponse, RoundTripError> {
        match self {
            Connection::Tcp(stream) => round_trip(stream, cmd).await,
            #[cfg(unix)]
            Connection::Unix(stream) => round_trip(stream, cmd).await,
        }
    }
}

/// Sends one command and reads its reply.
async fn round_trip<S>(
    stream: &mut S,
    cmd: &RemoteCommand,
) -> Result<RemoteResponse, RoundTripError>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin,
{
    send_message(stream, cmd).await.map_err(|e| match e {
        // Writing failed: the server never received a whole command.
        DistributedError::Io(_) => RoundTripError::BeforeReply(e),
        other => RoundTripError::Failed(other),
    })?;

    receive_message(stream)
        .await
        .map_err(|frame_error| match frame_error {
            FrameError::NotStarted(e) => RoundTripError::BeforeReply(DistributedError::Io(e)),
            FrameError::Failed(e) => RoundTripError::Failed(e),
        })
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
