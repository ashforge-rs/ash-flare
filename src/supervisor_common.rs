//! Shared internal utilities for supervisor implementations

use crate::types::{ChildExitReason, ChildId};
use crate::worker::Worker;
use tokio::sync::mpsc;

/// Message sent when a worker terminates
pub(crate) struct WorkerTermination {
    pub id: ChildId,
    pub reason: ChildExitReason,
}

/// Runs a worker with initialization, execution, and shutdown lifecycle
pub(crate) async fn run_worker<W: Worker, Cmd>(
    supervisor_name: String,
    worker_id: ChildId,
    mut worker: W,
    control_tx: mpsc::UnboundedSender<Cmd>,
    init_tx: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
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

    // Run the worker's main loop
    let exit_reason = match worker.run().await {
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
