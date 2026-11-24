//! Typed mailboxes: requests and replies, surviving worker restarts, and
//! sharing one queue between a pool of workers.

use ash_flare::{
    CallError, Mailbox, MailboxConfig, Reply, RestartBackoff, RestartPolicy, SupervisorHandle,
    SupervisorSpec, Worker, mailbox,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

enum Command {
    Add(u64),
    Get(Reply<u64>),
    Crash,
    Ignore(Reply<u64>),
}

/// Keeps a running total; `Crash` makes it fail, `Ignore` drops the reply.
struct Accumulator {
    inbox: Mailbox<Command>,
    total: u64,
    starts: Arc<AtomicUsize>,
}

impl Worker for Accumulator {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        while let Some(command) = self.inbox.recv().await {
            match command {
                Command::Add(n) => self.total += n,
                Command::Get(reply) => reply.send(self.total),
                Command::Crash => return Err(std::io::Error::other("asked to crash")),
                Command::Ignore(reply) => drop(reply),
            }
        }
        Ok(())
    }
}

fn accumulator(
    inbox: &Mailbox<Command>,
    starts: &Arc<AtomicUsize>,
) -> impl Fn() -> Accumulator + Send + Sync + 'static {
    let (inbox, starts) = (inbox.clone(), Arc::clone(starts));
    move || Accumulator {
        inbox: inbox.clone(),
        total: 0,
        starts: Arc::clone(&starts),
    }
}

const WAIT: Duration = Duration::from_secs(1);

#[tokio::test]
async fn call_returns_the_reply() {
    let (handle, inbox) = mailbox::<Command>(MailboxConfig::default());
    let starts = Arc::new(AtomicUsize::new(0));
    let supervisor = SupervisorHandle::start(SupervisorSpec::new("calls").with_worker(
        "acc",
        accumulator(&inbox, &starts),
        RestartPolicy::Permanent,
    ))
    .expect("valid spec");

    handle.send(Command::Add(40)).await.unwrap();
    handle.send(Command::Add(2)).await.unwrap();
    assert_eq!(handle.call(Command::Get, WAIT).await, Ok(42));
    supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn call_reports_a_dropped_reply_and_a_timeout() {
    let (handle, inbox) = mailbox::<Command>(MailboxConfig::default());
    let starts = Arc::new(AtomicUsize::new(0));
    let supervisor = SupervisorHandle::start(SupervisorSpec::new("no-reply").with_worker(
        "acc",
        accumulator(&inbox, &starts),
        RestartPolicy::Permanent,
    ))
    .expect("valid spec");

    assert_eq!(
        handle.call(Command::Ignore, WAIT).await,
        Err(CallError::NoReply)
    );

    // Nobody is receiving from this mailbox, so the call can only time out.
    let (unanswered, _kept_alive) = mailbox::<Command>(MailboxConfig::default());
    let timeout = Duration::from_millis(30);
    assert_eq!(
        unanswered.call(Command::Get, timeout).await,
        Err(CallError::Timeout(timeout))
    );

    supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn call_on_a_closed_mailbox_fails() {
    let (handle, inbox) = mailbox::<Command>(MailboxConfig::default());
    drop(inbox);
    assert!(matches!(
        handle.call(Command::Get, WAIT).await,
        Err(CallError::Closed(_))
    ));
}

/// The handle stays valid across a restart, and messages sent while the
/// worker was down are handled by its replacement.
#[tokio::test]
async fn the_mailbox_survives_a_worker_restart() {
    let (handle, inbox) = mailbox::<Command>(MailboxConfig::default());
    let starts = Arc::new(AtomicUsize::new(0));
    let supervisor = SupervisorHandle::start(
        SupervisorSpec::new("restart")
            .with_restart_backoff(RestartBackoff::exponential(
                Duration::from_millis(50),
                Duration::from_millis(50),
            ))
            .with_worker(
                "acc",
                accumulator(&inbox, &starts),
                RestartPolicy::Permanent,
            ),
    )
    .expect("valid spec");

    handle.send(Command::Add(1)).await.unwrap();
    handle.send(Command::Crash).await.unwrap();
    // Sent while the worker is down, backing off before its restart.
    handle.send(Command::Add(5)).await.unwrap();

    assert_eq!(
        handle.call(Command::Get, WAIT).await,
        Ok(5),
        "state resets, messages do not"
    );
    assert_eq!(starts.load(Ordering::SeqCst), 2);
    supervisor.shutdown().await.unwrap();
}

/// Workers sharing one mailbox split its messages between them, each message
/// handled exactly once.
#[tokio::test]
async fn clones_of_a_mailbox_form_a_pool() {
    struct Member {
        inbox: Mailbox<u64>,
        handled: Arc<AtomicUsize>,
        sum: Arc<AtomicUsize>,
    }
    impl Worker for Member {
        type Error = std::io::Error;
        async fn run(&mut self) -> Result<(), Self::Error> {
            while let Some(n) = self.inbox.recv().await {
                self.handled.fetch_add(1, Ordering::SeqCst);
                self.sum.fetch_add(n as usize, Ordering::SeqCst);
                tokio::task::yield_now().await;
            }
            Ok(())
        }
    }

    let (handle, inbox) = mailbox::<u64>(MailboxConfig::unbounded());
    let counters: Vec<Arc<AtomicUsize>> = (0..3).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    let sum = Arc::new(AtomicUsize::new(0));
    let mut spec = SupervisorSpec::new("pool");
    for (i, handled) in counters.iter().enumerate() {
        let (inbox, handled, sum) = (inbox.clone(), Arc::clone(handled), Arc::clone(&sum));
        spec = spec.with_worker(
            format!("member-{i}"),
            move || Member {
                inbox: inbox.clone(),
                handled: Arc::clone(&handled),
                sum: Arc::clone(&sum),
            },
            RestartPolicy::Permanent,
        );
    }
    let supervisor = SupervisorHandle::start(spec).expect("valid spec");

    for n in 1u64..=300 {
        handle.send(n).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let total: usize = counters.iter().map(|c| c.load(Ordering::SeqCst)).sum();
    assert_eq!(total, 300, "every message handled exactly once");
    assert_eq!(sum.load(Ordering::SeqCst), (1..=300).sum::<usize>());
    supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn string_mailboxes_still_work_by_default() {
    let (handle, inbox): (ash_flare::MailboxHandle, Mailbox) = mailbox(MailboxConfig::bounded(4));
    handle.send("hello").await.unwrap();
    assert_eq!(inbox.recv().await.as_deref(), Some("hello"));
}
