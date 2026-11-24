//! Pools of identical workers and children started under generated ids.

use ash_flare::{
    ChildSpec, Mailbox, MailboxConfig, Reply, RestartPolicy, ShutdownSignal, SupervisorHandle,
    SupervisorSpec, Worker, mailbox,
};
use std::collections::HashSet;
use std::time::Duration;

struct Idle(ShutdownSignal);
impl Worker for Idle {
    type Error = std::io::Error;
    async fn run(&mut self) -> Result<(), Self::Error> {
        self.0.cancelled().await;
        Ok(())
    }
}

#[tokio::test]
async fn unique_children_get_distinct_generated_ids() {
    let handle = SupervisorHandle::start(SupervisorSpec::new("dynamic")).expect("valid spec");

    let mut ids = HashSet::new();
    for _ in 0..50 {
        let id = handle
            .start_unique_child(ChildSpec::new("conn", Idle).restart(RestartPolicy::Temporary))
            .await
            .expect("started");
        assert!(id.starts_with("conn-"), "{id}");
        ids.insert(id);
    }
    assert_eq!(ids.len(), 50);
    assert_eq!(handle.which_children().await.unwrap().len(), 50);

    // A generated id never collides with one chosen by hand: take the next
    // generated id by hand first, then ask for another.
    let generated = handle
        .start_unique_child(ChildSpec::new("job", Idle))
        .await
        .unwrap();
    let taken_by_hand = format!(
        "job-{}",
        generated["job-".len()..].parse::<u64>().unwrap() + 1
    );
    handle
        .start_child(taken_by_hand.clone(), Idle, RestartPolicy::Temporary)
        .await
        .unwrap();
    let next = handle
        .start_unique_child(ChildSpec::new("job", Idle))
        .await
        .unwrap();
    assert!(next.starts_with("job-") && next != taken_by_hand, "{next}");
    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_unique_starts_do_not_collide() {
    let handle = SupervisorHandle::start(SupervisorSpec::new("concurrent")).expect("valid spec");
    let starts: Vec<_> = (0..20)
        .map(|_| {
            let handle = handle.clone();
            tokio::spawn(async move {
                handle
                    .start_unique_child(ChildSpec::new("w", Idle))
                    .await
                    .unwrap()
            })
        })
        .collect();

    let mut ids = HashSet::new();
    for start in starts {
        ids.insert(start.await.unwrap());
    }
    assert_eq!(ids.len(), 20);
    handle.shutdown().await.unwrap();
}

enum Job {
    WhoAreYou(Reply<usize>),
}

/// Answers with its pool index.
struct Member {
    index: usize,
    inbox: Mailbox<Job>,
}
impl Worker for Member {
    type Error = std::io::Error;
    async fn run(&mut self) -> Result<(), Self::Error> {
        while let Some(Job::WhoAreYou(reply)) = self.inbox.recv().await {
            reply.send(self.index);
            // Give the other members a turn.
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok(())
    }
}

#[tokio::test]
async fn a_worker_pool_gets_indexed_ids_and_shares_its_mailbox() {
    let (jobs, inbox) = mailbox::<Job>(MailboxConfig::unbounded());
    let spec = SupervisorSpec::new("pool").with_worker_pool(
        "member",
        3,
        move |index| Member {
            index,
            inbox: inbox.clone(),
        },
        RestartPolicy::Permanent,
    );
    let handle = SupervisorHandle::start(spec).expect("valid spec");

    let ids: Vec<String> = handle
        .which_children()
        .await
        .unwrap()
        .into_iter()
        .map(|c| c.id)
        .collect();
    assert_eq!(ids, vec!["member-0", "member-1", "member-2"]);

    let calls: Vec<_> = (0..9)
        .map(|_| {
            let jobs = jobs.clone();
            tokio::spawn(async move {
                jobs.call(Job::WhoAreYou, Duration::from_secs(1))
                    .await
                    .unwrap()
            })
        })
        .collect();
    let mut answered_by = HashSet::new();
    for call in calls {
        answered_by.insert(call.await.unwrap());
    }
    assert_eq!(
        answered_by,
        HashSet::from([0, 1, 2]),
        "the work was spread over the pool"
    );
    handle.shutdown().await.unwrap();
}
