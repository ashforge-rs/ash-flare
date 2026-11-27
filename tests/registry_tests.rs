//! Looking workers up by name.

use ash_flare::mailbox::{Mailbox, MailboxConfig, MailboxHandle, mailbox};
use ash_flare::{
    Registration, Registry, RegistryError, Reply, RestartBackoff, RestartPolicy, SupervisorHandle,
    SupervisorSpec, Worker,
};
use std::time::Duration;

#[test]
fn whereis_finds_a_registered_value_of_the_right_type() {
    let registry = Registry::new();
    let _name = registry.register("answer", 42_u32);

    assert_eq!(registry.whereis::<u32>("answer"), Some(42));
    assert_eq!(registry.whereis::<String>("answer"), None, "wrong type");
    assert_eq!(registry.whereis::<u32>("missing"), None);
}

#[test]
fn dropping_the_registration_frees_the_name() {
    let registry = Registry::new();
    let name = registry.register("temp", 1_u8);
    assert!(registry.is_registered("temp"));
    drop(name);
    assert!(!registry.is_registered("temp"));
}

#[test]
fn a_stale_registration_leaves_its_replacement_alone() {
    let registry = Registry::new();
    let old = registry.register("worker", "old instance");
    let new = registry.register("worker", "new instance");

    drop(old);
    assert_eq!(registry.whereis::<&str>("worker"), Some("new instance"));
    drop(new);
    assert!(!registry.is_registered("worker"));
}

#[test]
fn try_register_refuses_a_taken_name() {
    let registry = Registry::new();
    let _held = registry.try_register("single", ()).expect("free");
    assert_eq!(
        registry
            .try_register("single", ())
            .map(|r| r.name().to_owned()),
        Err(RegistryError::AlreadyRegistered("single".to_owned()))
    );
}

enum Ping {
    Ping(Reply<&'static str>),
    Crash,
}

/// Registers its mailbox under "pinger" for as long as it runs.
struct Pinger {
    inbox: Mailbox<Ping>,
    handle: MailboxHandle<Ping>,
    registry: Registry,
    name: Option<Registration>,
}

impl Worker for Pinger {
    type Error = std::io::Error;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        self.name = Some(self.registry.register("pinger", self.handle.clone()));
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        while let Some(message) = self.inbox.recv().await {
            match message {
                Ping::Ping(reply) => reply.send("pong"),
                Ping::Crash => return Err(std::io::Error::other("crash")),
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn a_registered_worker_is_found_again_after_a_restart() {
    let registry = Registry::new();
    let (handle, inbox) = mailbox::<Ping>(MailboxConfig::default());
    let factory_registry = registry.clone();
    let spec = SupervisorSpec::new("named")
        .with_restart_backoff(RestartBackoff::none())
        .with_worker(
            "pinger",
            move || Pinger {
                inbox: inbox.clone(),
                handle: handle.clone(),
                registry: factory_registry.clone(),
                name: None,
            },
            RestartPolicy::Permanent,
        );
    let supervisor = SupervisorHandle::start(spec).expect("valid spec");
    tokio::time::sleep(Duration::from_millis(20)).await;

    let pinger = registry
        .whereis::<MailboxHandle<Ping>>("pinger")
        .expect("registered");
    pinger.send(Ping::Crash).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let pinger = registry
        .whereis::<MailboxHandle<Ping>>("pinger")
        .expect("registered again by the restarted worker");
    assert_eq!(
        pinger.call(Ping::Ping, Duration::from_secs(1)).await,
        Ok("pong")
    );

    supervisor.shutdown().await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !registry.is_registered("pinger"),
        "the name went away with the worker"
    );
}
