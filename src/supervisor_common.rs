//! Shared internal utilities for supervisor implementations

use crate::restart::{BackoffTracker, RestartPolicy, RestartStrategy, RestartTracker};
use crate::types::{ChildExitReason, ChildId, ShutdownSignal};
use crate::worker::Worker;
use std::time::Duration;
use tokio::sync::mpsc;

/// What a supervisor should do after one of its children terminated.
///
/// Produced by [`decide_restart`], which owns the supervision semantics shared
/// by the stateless and stateful supervisors. Keeping this in one place means a
/// change to restart behaviour cannot be applied to one runtime and forgotten in
/// the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RestartDecision {
    /// The supervisor stopped this child itself; there is nothing to decide.
    /// The child stays in the children list, untouched.
    Ignore,
    /// The child's policy says not to restart; drop it from the children list.
    Drop,
    /// Restart intensity was exceeded; shut the whole supervisor down.
    Escalate,
    /// Restart, after waiting `delay`, using `strategy`.
    Restart {
        /// Backoff to wait before respawning.
        delay: Duration,
        /// Strategy determining which children are restarted.
        strategy: RestartStrategy,
    },
}

/// Decides what to do when a child terminates.
///
/// This is the single source of truth for supervision semantics: policy check,
/// restart-intensity accounting, and backoff. It is deliberately free of any
/// runtime-specific types so both supervisor implementations can share it.
///
/// `policy` is `None` for nested supervisors, which are always treated as
/// permanent.
pub(crate) fn decide_restart(
    policy: Option<RestartPolicy>,
    reason: ChildExitReason,
    strategy: RestartStrategy,
    restart_tracker: &mut RestartTracker,
    backoff_tracker: &mut BackoffTracker,
    child_id: &str,
) -> RestartDecision {
    // A `Shutdown` exit is the supervisor's own doing: a worker only reports it
    // after the supervisor asked it to stop. Feeding that back through the
    // policy would treat a deliberate stop as a failure - under `OneForAll` and
    // `RestForOne` the stops performed *as part of* a restart would each trigger
    // another restart round, and a healthy `Transient`/`Temporary` sibling would
    // be dropped from supervision entirely.
    if reason == ChildExitReason::Shutdown {
        return RestartDecision::Ignore;
    }

    // Nested supervisors have no policy of their own and are always permanent.
    let should_restart = match policy {
        None | Some(RestartPolicy::Permanent) => true,
        Some(RestartPolicy::Temporary) => false,
        Some(RestartPolicy::Transient) => reason == ChildExitReason::Abnormal,
    };

    if !should_restart {
        backoff_tracker.reset_child(child_id);
        return RestartDecision::Drop;
    }

    if restart_tracker.record_restart() {
        return RestartDecision::Escalate;
    }

    RestartDecision::Restart {
        delay: backoff_tracker.next_delay(child_id),
        strategy,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::restart::{RestartBackoff, RestartIntensity};

    fn trackers(max_restarts: usize) -> (RestartTracker, BackoffTracker) {
        (
            RestartTracker::new(RestartIntensity::new(max_restarts, 60)),
            BackoffTracker::new(RestartBackoff::none()),
        )
    }

    #[test]
    fn permanent_restarts_on_any_reason() {
        for reason in [ChildExitReason::Normal, ChildExitReason::Abnormal] {
            let (mut restart, mut backoff) = trackers(10);
            let decision = decide_restart(
                Some(RestartPolicy::Permanent),
                reason,
                RestartStrategy::OneForOne,
                &mut restart,
                &mut backoff,
                "c",
            );
            assert!(matches!(decision, RestartDecision::Restart { .. }));
        }
    }

    #[test]
    fn temporary_never_restarts() {
        for reason in [ChildExitReason::Normal, ChildExitReason::Abnormal] {
            let (mut restart, mut backoff) = trackers(10);
            let decision = decide_restart(
                Some(RestartPolicy::Temporary),
                reason,
                RestartStrategy::OneForOne,
                &mut restart,
                &mut backoff,
                "c",
            );
            assert_eq!(decision, RestartDecision::Drop);
        }
    }

    #[test]
    fn transient_restarts_only_on_abnormal_exit() {
        let (mut restart, mut backoff) = trackers(10);
        assert_eq!(
            decide_restart(
                Some(RestartPolicy::Transient),
                ChildExitReason::Normal,
                RestartStrategy::OneForOne,
                &mut restart,
                &mut backoff,
                "c",
            ),
            RestartDecision::Drop
        );

        let (mut restart, mut backoff) = trackers(10);
        assert!(matches!(
            decide_restart(
                Some(RestartPolicy::Transient),
                ChildExitReason::Abnormal,
                RestartStrategy::OneForOne,
                &mut restart,
                &mut backoff,
                "c",
            ),
            RestartDecision::Restart { .. }
        ));
    }

    #[test]
    fn nested_supervisors_are_always_permanent() {
        let (mut restart, mut backoff) = trackers(10);
        assert!(matches!(
            decide_restart(
                None,
                ChildExitReason::Normal,
                RestartStrategy::OneForOne,
                &mut restart,
                &mut backoff,
                "sup",
            ),
            RestartDecision::Restart { .. }
        ));
    }

    #[test]
    fn supervisor_initiated_shutdown_is_ignored() {
        for policy in [
            None,
            Some(RestartPolicy::Permanent),
            Some(RestartPolicy::Transient),
            Some(RestartPolicy::Temporary),
        ] {
            for strategy in [
                RestartStrategy::OneForOne,
                RestartStrategy::OneForAll,
                RestartStrategy::RestForOne,
            ] {
                let (mut restart, mut backoff) = trackers(10);
                assert_eq!(
                    decide_restart(
                        policy,
                        ChildExitReason::Shutdown,
                        strategy,
                        &mut restart,
                        &mut backoff,
                        "c",
                    ),
                    RestartDecision::Ignore,
                    "a stop the supervisor asked for is not a failure ({policy:?}, {strategy:?})"
                );
            }
        }
    }

    #[test]
    fn ignored_shutdowns_do_not_consume_restart_intensity() {
        let (mut restart, mut backoff) = trackers(1);

        for _ in 0..10 {
            let _ignored = decide_restart(
                Some(RestartPolicy::Permanent),
                ChildExitReason::Shutdown,
                RestartStrategy::OneForAll,
                &mut restart,
                &mut backoff,
                "c",
            );
        }

        assert!(
            matches!(
                decide_restart(
                    Some(RestartPolicy::Permanent),
                    ChildExitReason::Abnormal,
                    RestartStrategy::OneForAll,
                    &mut restart,
                    &mut backoff,
                    "c",
                ),
                RestartDecision::Restart { .. }
            ),
            "deliberate stops must not burn the restart budget"
        );
    }

    #[test]
    fn exceeding_intensity_escalates() {
        let (mut restart, mut backoff) = trackers(2);
        for _ in 0..2 {
            assert!(matches!(
                decide_restart(
                    Some(RestartPolicy::Permanent),
                    ChildExitReason::Abnormal,
                    RestartStrategy::OneForOne,
                    &mut restart,
                    &mut backoff,
                    "c",
                ),
                RestartDecision::Restart { .. }
            ));
        }

        assert_eq!(
            decide_restart(
                Some(RestartPolicy::Permanent),
                ChildExitReason::Abnormal,
                RestartStrategy::OneForOne,
                &mut restart,
                &mut backoff,
                "c",
            ),
            RestartDecision::Escalate,
            "third restart must exceed a limit of 2"
        );
    }

    #[test]
    fn backoff_grows_per_child_and_resets_on_drop() {
        let mut restart = RestartTracker::new(RestartIntensity::new(100, 60));
        let mut backoff = BackoffTracker::new(RestartBackoff::exponential(
            Duration::from_millis(100),
            Duration::from_secs(10),
        ));

        let delay_of = |d: RestartDecision| match d {
            RestartDecision::Restart { delay, .. } => delay,
            other => panic!("expected restart, got {other:?}"),
        };

        let first = delay_of(decide_restart(
            Some(RestartPolicy::Permanent),
            ChildExitReason::Abnormal,
            RestartStrategy::OneForOne,
            &mut restart,
            &mut backoff,
            "a",
        ));
        let second = delay_of(decide_restart(
            Some(RestartPolicy::Permanent),
            ChildExitReason::Abnormal,
            RestartStrategy::OneForOne,
            &mut restart,
            &mut backoff,
            "a",
        ));
        assert_eq!(first, Duration::from_millis(100));
        assert_eq!(second, Duration::from_millis(200), "backoff must grow");

        // A different child has its own independent backoff.
        let other = delay_of(decide_restart(
            Some(RestartPolicy::Permanent),
            ChildExitReason::Abnormal,
            RestartStrategy::OneForOne,
            &mut restart,
            &mut backoff,
            "b",
        ));
        assert_eq!(other, Duration::from_millis(100), "per-child tracking");

        // Dropping a child clears its history.
        let _dropped = decide_restart(
            Some(RestartPolicy::Temporary),
            ChildExitReason::Normal,
            RestartStrategy::OneForOne,
            &mut restart,
            &mut backoff,
            "a",
        );
        let after_reset = delay_of(decide_restart(
            Some(RestartPolicy::Permanent),
            ChildExitReason::Abnormal,
            RestartStrategy::OneForOne,
            &mut restart,
            &mut backoff,
            "a",
        ));
        assert_eq!(
            after_reset,
            Duration::from_millis(100),
            "dropping a child must reset its backoff"
        );
    }

    #[test]
    fn strategy_is_passed_through_unchanged() {
        for strategy in [
            RestartStrategy::OneForOne,
            RestartStrategy::OneForAll,
            RestartStrategy::RestForOne,
        ] {
            let (mut restart, mut backoff) = trackers(10);
            let decision = decide_restart(
                Some(RestartPolicy::Permanent),
                ChildExitReason::Abnormal,
                strategy,
                &mut restart,
                &mut backoff,
                "c",
            );
            match decision {
                RestartDecision::Restart { strategy: got, .. } => assert_eq!(got, strategy),
                other => panic!("expected restart, got {other:?}"),
            }
        }
    }
}

/// Message sent when a worker terminates
pub(crate) struct WorkerTermination {
    pub id: ChildId,
    pub reason: ChildExitReason,
}

/// Runs a worker with initialization, execution, and shutdown lifecycle
///
/// When `shutdown` is triggered the worker's `run` future is dropped at its next
/// await point, but `shutdown` is still awaited so cleanup runs. This is what
/// makes graceful termination different from aborting the task outright.
pub(crate) async fn run_worker<W: Worker, Cmd>(
    supervisor_name: String,
    worker_id: ChildId,
    mut worker: W,
    control_tx: mpsc::UnboundedSender<Cmd>,
    init_tx: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
    shutdown: ShutdownSignal,
) where
    Cmd: From<WorkerTermination>,
{
    let qualified_name = format!("{supervisor_name}/{worker_id}");

    // Initialize the worker
    match worker.initialize().await {
        Ok(()) => {
            // Send initialization success confirmation if linked
            if let Some(tx) = init_tx {
                let _send = tx.send(Ok(()));
            }
        }
        Err(err) => {
            tracing::error!(
                worker = %qualified_name,
                error = %err,
                "worker initialization failed"
            );
            // Send initialization failure if linked
            if let Some(tx) = init_tx {
                let _send = tx.send(Err(err.to_string()));
            }
            let _send = control_tx.send(
                WorkerTermination {
                    id: worker_id,
                    reason: ChildExitReason::Abnormal,
                }
                .into(),
            );
            return;
        }
    }

    tracing::debug!(worker = %qualified_name, "worker started");

    // Run the worker's main loop, racing it against the shutdown signal. A
    // worker that observes the signal returns on its own; one that ignores it
    // has its `run` future dropped here, but still gets its `shutdown` hook.
    let exit_reason = tokio::select! {
        biased;

        () = shutdown.cancelled() => {
            tracing::debug!(worker = %qualified_name, "worker shutdown requested");
            ChildExitReason::Shutdown
        }
        result = worker.run() => match result {
            Ok(()) => {
                tracing::debug!(worker = %qualified_name, "worker completed normally");
                ChildExitReason::Normal
            }
            Err(err) => {
                tracing::warn!(
                    worker = %qualified_name,
                    error = %err,
                    "worker failed"
                );
                ChildExitReason::Abnormal
            }
        },
    };

    // Shutdown the worker
    if let Err(err) = worker.shutdown().await {
        tracing::error!(
            worker = %qualified_name,
            error = %err,
            "worker shutdown failed"
        );
    }

    tracing::debug!(worker = %qualified_name, "worker stopped");

    // Notify supervisor of termination
    let _send = control_tx.send(
        WorkerTermination {
            id: worker_id,
            reason: exit_reason,
        }
        .into(),
    );
}
