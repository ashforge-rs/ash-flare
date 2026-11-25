//! A key-value server in the style of an OTP `gen_server`.
//!
//! This example demonstrates:
//! - A typed `Mailbox<M>` whose messages are an enum of everything the worker
//!   understands
//! - `MailboxHandle::call` with a `Reply` for requests that need an answer, and
//!   `send` for one-way messages (OTP's `cast`)
//! - `CallError::NoReply` when the worker crashes before answering
//! - A `Registry`, so clients find the server by name instead of being handed
//!   its handle (`register` / `whereis`)
//! - A mailbox that survives restarts: the server's state is lost when it
//!   crashes, but its handle keeps working, and messages sent while it was
//!   restarting are waiting for its replacement
//!
//! Run with: `cargo run --example kv_server`

use ash_flare::{
    CallError, Mailbox, MailboxConfig, MailboxHandle, Registration, Registry, Reply, RestartPolicy,
    SupervisorEvent, SupervisorHandle, SupervisorSpec, Worker, mailbox_named,
};
use std::collections::HashMap;
use std::time::Duration;

const CALL_TIMEOUT: Duration = Duration::from_secs(1);

/// Everything the key-value server understands.
enum KvRequest {
    /// Store a value; no answer (a cast).
    Put { key: String, value: String },
    /// Look a value up (a call).
    Get {
        key: String,
        reply: Reply<Option<String>>,
    },
    /// How many keys are stored (a call).
    Len(Reply<usize>),
    /// Crash while handling a call, dropping its `Reply` unanswered.
    Crash(Reply<()>),
}

type KvHandle = MailboxHandle<KvRequest>;

struct KvServer {
    inbox: Mailbox<KvRequest>,
    handle: KvHandle,
    registry: Registry,
    /// Keeps the name registered for as long as this instance lives.
    name: Option<Registration>,
    data: HashMap<String, String>,
}

impl Worker for KvServer {
    type Error = String;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        self.name = Some(self.registry.register("kv", self.handle.clone()));
        println!("  [kv] started with an empty store, registered as \"kv\"");
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        while let Some(request) = self.inbox.recv().await {
            match request {
                KvRequest::Put { key, value } => {
                    println!("  [kv] put {key} = {value}");
                    self.data.insert(key, value);
                }
                KvRequest::Get { key, reply } => reply.send(self.data.get(&key).cloned()),
                KvRequest::Len(reply) => reply.send(self.data.len()),
                KvRequest::Crash(_reply) => {
                    println!("  [kv] crashing with {} keys in memory", self.data.len());
                    return Err("asked to crash".to_owned());
                }
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Key-value server: call, cast and a registry ===\n");

    let registry = Registry::new();
    let (handle, inbox) = mailbox_named::<KvRequest>(MailboxConfig::default(), "kv");

    // Every instance the factory builds shares the same mailbox and registry.
    let factory_registry = registry.clone();
    let spec = SupervisorSpec::new("kv-app")
        .with_restart_delay(Duration::from_millis(200))
        .with_worker(
            "kv",
            move || KvServer {
                inbox: inbox.clone(),
                handle: handle.clone(),
                registry: factory_registry.clone(),
                name: None,
                data: HashMap::new(),
            },
            RestartPolicy::Permanent,
        );
    let supervisor = SupervisorHandle::start(spec)?;
    let mut events = supervisor.subscribe();

    // Clients only know the name.
    let kv = wait_for_name(&registry, "kv").await;
    println!("client: found \"kv\" in the registry\n");

    println!("1. Casts and calls");
    kv.send(KvRequest::Put {
        key: "language".into(),
        value: "rust".into(),
    })
    .await?;
    kv.send(KvRequest::Put {
        key: "inspired-by".into(),
        value: "erlang".into(),
    })
    .await?;
    let language = kv
        .call(|reply| get("language", reply), CALL_TIMEOUT)
        .await?;
    let missing = kv.call(|reply| get("missing", reply), CALL_TIMEOUT).await?;
    let len = kv.call(KvRequest::Len, CALL_TIMEOUT).await?;
    println!("client: get(language) = {language:?}, get(missing) = {missing:?}, len = {len}\n");

    println!("2. A call whose handler crashes");
    match kv.call(KvRequest::Crash, CALL_TIMEOUT).await {
        Err(CallError::NoReply) => println!("client: call failed with NoReply, as expected"),
        other => println!("client: unexpected result {other:?}"),
    }

    // Sent while the server is down: it waits in the mailbox.
    kv.send(KvRequest::Put {
        key: "sent-during-restart".into(),
        value: "kept".into(),
    })
    .await?;
    println!("client: sent a put while the server was restarting\n");

    wait_for_restart(&mut events, "kv").await;

    println!("\n3. After the restart");
    let len = kv.call(KvRequest::Len, CALL_TIMEOUT).await?;
    let kept = kv
        .call(|reply| get("sent-during-restart", reply), CALL_TIMEOUT)
        .await?;
    let lost = kv
        .call(|reply| get("language", reply), CALL_TIMEOUT)
        .await?;
    println!("client: the same handle still works: len = {len}");
    println!("client: get(sent-during-restart) = {kept:?} (queued, not lost)");
    println!("client: get(language) = {lost:?} (in-memory state died with the old instance)");
    println!(
        "client: \"kv\" still registered: {}",
        registry.is_registered("kv")
    );

    supervisor.shutdown().await?;
    println!("\nDone.");
    Ok(())
}

fn get(key: &str, reply: Reply<Option<String>>) -> KvRequest {
    KvRequest::Get {
        key: key.to_owned(),
        reply,
    }
}

/// Polls the registry until the server has registered itself.
async fn wait_for_name(registry: &Registry, name: &str) -> KvHandle {
    loop {
        if let Some(handle) = registry.whereis::<KvHandle>(name) {
            return handle;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_restart(
    events: &mut tokio::sync::broadcast::Receiver<SupervisorEvent>,
    child: &str,
) {
    while let Ok(event) = events.recv().await {
        match event {
            SupervisorEvent::ChildTerminated { id, reason } if &*id == child => {
                println!("supervisor: {id} terminated ({reason:?})");
            }
            SupervisorEvent::ChildRestarted { id } if &*id == child => {
                println!("supervisor: {id} restarted");
                return;
            }
            _ => {}
        }
    }
}
