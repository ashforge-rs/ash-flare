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
    let qualified_name = format!("{}/{}", supervisor_name, worker_id);

    // Initialize the worker
    match worker.initialize().await {
        Ok(()) => {
            // Send initialization success confirmation if linked
            if let Some(tx) = init_tx {
                let _ = tx.send(Ok(()));
            }
        }
        Err(err) => {
            slog::error!(slog_scope::logger(), "worker initialization failed";
                "worker" => &qualified_name,
                "error" => %err
            );
            // Send initialization failure if linked
            if let Some(tx) = init_tx {
                let _ = tx.send(Err(err.to_string()));
            }
            let _ = control_tx.send(
                WorkerTermination {
                    id: worker_id,
                    reason: ChildExitReason::Abnormal,
                }
                .into(),
            );
            return;
        }
    }

    slog::debug!(slog_scope::logger(), "worker started"; "worker" => &qualified_name);

    // Run the worker's main loop
    let exit_reason = match worker.run().await {
        Ok(()) => {
            slog::debug!(slog_scope::logger(), "worker completed normally"; "worker" => &qualified_name);
            ChildExitReason::Normal
        }
        Err(err) => {
            slog::warn!(slog_scope::logger(), "worker failed";
                "worker" => &qualified_name,
                "error" => %err
            );
            ChildExitReason::Abnormal
        }
    };

    // Shutdown the worker
    if let Err(err) = worker.shutdown().await {
        slog::error!(slog_scope::logger(), "worker shutdown failed";
            "worker" => &qualified_name,
            "error" => %err
        );
    }

    slog::debug!(slog_scope::logger(), "worker stopped"; "worker" => &qualified_name);

    // Notify supervisor of termination
    let _ = control_tx.send(
        WorkerTermination {
            id: worker_id,
            reason: exit_reason,
        }
        .into(),
    );
}
