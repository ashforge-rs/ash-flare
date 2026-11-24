//! The connection handshake: protocol versioning and cookie authentication.
#![cfg(unix)]

use ash_flare::distributed::{
    ClientHello, DistributedError, PROTOCOL_MAGIC, RemoteSupervisorHandle, ServerHello,
    SupervisorServer,
};
use ash_flare::{SupervisorHandle, SupervisorSpec};
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Starts a server on a fresh Unix socket and returns its path.
async fn serve(name: &str, cookie: Option<&str>) -> (SupervisorHandle, PathBuf) {
    let path = std::env::temp_dir().join(format!("ash-hs-{name}-{}.sock", std::process::id()));
    let handle = SupervisorHandle::start(SupervisorSpec::new(name)).expect("valid spec");
    let mut server = SupervisorServer::new(handle.clone());
    if let Some(cookie) = cookie {
        server = server.with_cookie(cookie);
    }
    let listen_path = path.clone();
    tokio::spawn(async move { server.listen_unix(listen_path).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (handle, path)
}

fn client(path: &PathBuf) -> RemoteSupervisorHandle {
    RemoteSupervisorHandle::unix(path.to_string_lossy().into_owned())
}

#[tokio::test]
async fn matching_cookies_authenticate() {
    let (handle, path) = serve("matching", Some("s3cret")).await;
    let status = client(&path)
        .with_cookie("s3cret")
        .status()
        .await
        .expect("authenticated");
    assert_eq!(status.name, "matching");
    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_wrong_cookie_is_refused() {
    let (handle, path) = serve("wrong", Some("s3cret")).await;
    let err = client(&path)
        .with_cookie("guess")
        .status()
        .await
        .unwrap_err();
    // The server's proof is checked first, so the client notices the mismatch.
    assert!(
        matches!(err, DistributedError::AuthenticationFailed(_)),
        "got {err:?}"
    );
    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_client_without_a_cookie_is_refused() {
    let (handle, path) = serve("required", Some("s3cret")).await;
    let err = client(&path).status().await.unwrap_err();
    assert!(
        matches!(err, DistributedError::AuthenticationRequired),
        "got {err:?}"
    );
    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_client_with_a_cookie_refuses_an_unauthenticated_server() {
    let (handle, path) = serve("impostor", None).await;
    let err = client(&path)
        .with_cookie("s3cret")
        .status()
        .await
        .unwrap_err();
    assert!(
        matches!(err, DistributedError::AuthenticationFailed(_)),
        "got {err:?}"
    );
    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn no_cookie_on_either_side_still_works() {
    let (handle, path) = serve("open", None).await;
    client(&path)
        .status()
        .await
        .expect("no authentication configured");
    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_client_on_another_protocol_version_is_told_so() {
    let (handle, path) = serve("versions", None).await;
    let mut stream = tokio::net::UnixStream::connect(&path).await.unwrap();

    let hello = ClientHello {
        magic: PROTOCOL_MAGIC,
        version: 99,
        nonce: [0; 32],
    };
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&hello).unwrap();
    stream
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .unwrap();
    stream.write_all(&bytes).await.unwrap();

    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await.unwrap();
    let mut body = vec![0u8; u32::from_be_bytes(len) as usize];
    stream.read_exact(&mut body).await.unwrap();
    let reply = rkyv::from_bytes::<ServerHello, rkyv::rancor::Error>(&body).unwrap();
    assert!(
        matches!(reply, ServerHello::Unsupported { server_version } if server_version == ash_flare::distributed::PROTOCOL_VERSION)
    );
    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_connection_that_skips_the_handshake_gets_no_answer() {
    use ash_flare::distributed::RemoteCommand;

    let (handle, path) = serve("skip", None).await;
    let mut stream = tokio::net::UnixStream::connect(&path).await.unwrap();

    // A pre-handshake client sends a command straight away.
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&RemoteCommand::Shutdown).unwrap();
    stream
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .unwrap();
    stream.write_all(&bytes).await.unwrap();

    let mut buf = [0u8; 4];
    let read = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut buf))
        .await
        .expect("server closes the connection")
        .unwrap();
    assert_eq!(read, 0, "connection closed without running the command");
    assert!(
        !handle.is_stopped(),
        "the shutdown command was not executed"
    );
    handle.shutdown().await.unwrap();
}

async fn write_frame<T>(stream: &mut tokio::net::UnixStream, value: &T)
where
    T: for<'a> rkyv::Serialize<
            rkyv::api::high::HighSerializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::rancor::Error,
            >,
        >,
{
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(value).unwrap();
    stream
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .unwrap();
    stream.write_all(&bytes).await.unwrap();
}

async fn read_frame(stream: &mut tokio::net::UnixStream) -> Vec<u8> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await.unwrap();
    let mut body = vec![0u8; u32::from_be_bytes(len) as usize];
    stream.read_exact(&mut body).await.unwrap();
    body
}

/// A client that ignores the server's proof and guesses its own is rejected
/// by the server, which then closes the connection.
#[tokio::test]
async fn the_server_rejects_a_forged_client_proof() {
    use ash_flare::distributed::{AuthResult, ClientProof, RemoteCommand};

    let (handle, path) = serve("forged", Some("s3cret")).await;
    let mut stream = tokio::net::UnixStream::connect(&path).await.unwrap();

    write_frame(
        &mut stream,
        &ClientHello {
            magic: PROTOCOL_MAGIC,
            version: ash_flare::distributed::PROTOCOL_VERSION,
            nonce: [7; 32],
        },
    )
    .await;
    let challenge = read_frame(&mut stream).await;
    assert!(matches!(
        rkyv::from_bytes::<ServerHello, rkyv::rancor::Error>(&challenge).unwrap(),
        ServerHello::Challenge { .. }
    ));

    write_frame(&mut stream, &ClientProof { proof: [0; 32] }).await;
    let verdict = read_frame(&mut stream).await;
    assert_eq!(
        rkyv::from_bytes::<AuthResult, rkyv::rancor::Error>(&verdict).unwrap(),
        AuthResult::Rejected
    );

    // Anything sent afterwards is not executed; the write may even fail
    // because the server has already closed the connection.
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&RemoteCommand::Shutdown).unwrap();
    let _ = stream.write_all(&(bytes.len() as u32).to_be_bytes()).await;
    let _ = stream.write_all(&bytes).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!handle.is_stopped());
    handle.shutdown().await.unwrap();
}
