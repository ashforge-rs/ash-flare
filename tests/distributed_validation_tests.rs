//! Regression tests for validated deserialization of untrusted network frames.
//!
//! Frames arrive from a socket, so a malformed or hostile payload must surface
//! as an error rather than being read as a trusted archive.

use ash_flare::distributed::{RemoteCommand, RemoteSupervisorHandle, SupervisorAddress};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Serves a single connection, discards the request, and replies with `payload`
/// behind a valid length prefix.
async fn serve_raw_reply(payload: Vec<u8>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();

    tokio::spawn(async move {
        let Ok((mut stream, _)) = listener.accept().await else {
            return;
        };

        // Drain the client's request frame.
        let mut header = [0u8; 4];
        if stream.read_exact(&mut header).await.is_err() {
            return;
        }
        let len = u32::from_be_bytes(header) as usize;
        let mut body = vec![0u8; len];
        if stream.read_exact(&mut body).await.is_err() {
            return;
        }

        let prefix = u32::try_from(payload.len()).unwrap_or(0).to_be_bytes();
        let _ = stream.write_all(&prefix).await;
        let _ = stream.write_all(&payload).await;
        let _ = stream.flush().await;
    });

    addr
}

#[tokio::test]
async fn garbage_payload_is_rejected() {
    let addr = serve_raw_reply(vec![0xAA; 64]).await;
    let handle = RemoteSupervisorHandle::new(SupervisorAddress::Tcp(addr));

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        handle.send_command(RemoteCommand::Status),
    )
    .await
    .expect("client must not hang on a malformed frame");

    assert!(
        result.is_err(),
        "a malformed archive must be rejected, not accepted as a valid response"
    );
}

#[tokio::test]
async fn truncated_payload_is_rejected() {
    let addr = serve_raw_reply(vec![0x00; 3]).await;
    let handle = RemoteSupervisorHandle::new(SupervisorAddress::Tcp(addr));

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        handle.send_command(RemoteCommand::Status),
    )
    .await
    .expect("client must not hang on a truncated frame");

    assert!(result.is_err(), "a truncated archive must be rejected");
}

#[tokio::test]
async fn empty_payload_is_rejected() {
    let addr = serve_raw_reply(Vec::new()).await;
    let handle = RemoteSupervisorHandle::new(SupervisorAddress::Tcp(addr));

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        handle.send_command(RemoteCommand::Status),
    )
    .await
    .expect("client must not hang on an empty frame");

    assert!(result.is_err(), "an empty archive must be rejected");
}

#[tokio::test]
async fn oversized_length_prefix_is_rejected_without_allocating() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();

    tokio::spawn(async move {
        let Ok((mut stream, _)) = listener.accept().await else {
            return;
        };
        let mut header = [0u8; 4];
        let _ = stream.read_exact(&mut header).await;
        let len = u32::from_be_bytes(header) as usize;
        let mut body = vec![0u8; len];
        let _ = stream.read_exact(&mut body).await;

        // Claim a frame far larger than the 10MB cap.
        let _ = stream.write_all(&u32::MAX.to_be_bytes()).await;
        let _ = stream.flush().await;
    });

    let handle = RemoteSupervisorHandle::new(SupervisorAddress::Tcp(addr));
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        handle.send_command(RemoteCommand::Status),
    )
    .await
    .expect("client must reject an oversized frame promptly");

    assert!(
        result.is_err(),
        "a length prefix beyond the cap must be rejected before allocating"
    );
}
