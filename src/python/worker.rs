//! Python worker implementation

use crate::worker::{Worker, WorkerError};
use pyo3::prelude::*;
use std::sync::Arc;

/// Python-compatible wrapper for Worker that accepts Python callables
#[derive(Clone)]
pub struct PyWorker {
    pub(crate) name: String,
    pub(crate) callable: Arc<Py<PyAny>>,
}

#[async_trait::async_trait]
impl Worker for PyWorker {
    type Error = WorkerError;

    async fn run(&mut self) -> Result<(), Self::Error> {
        // Execute Python callable in a dedicated thread for true parallelism
        // This allows 1000s of workers to run concurrently without blocking
        // each other (e.g., when using time.sleep())
        let callable = self.callable.clone();
        let name = self.name.clone();

        // Spawn a dedicated OS thread for this worker
        let handle = std::thread::spawn(move || {
            Python::with_gil(|py| {
                callable
                    .call0(py)
                    .map(|_| ()) // Discard the return value
                    .map_err(|e| {
                        WorkerError::WorkerFailed(format!("Worker '{}' failed: {}", name, e))
                    })
            })
        });

        // Wait for the thread to complete asynchronously (don't block the runtime)
        tokio::task::spawn_blocking(move || {
            handle
                .join()
                .map_err(|e| WorkerError::WorkerFailed(format!("Worker thread panicked: {:?}", e)))
        })
        .await
        .map_err(|e| WorkerError::WorkerFailed(format!("Failed to join worker task: {}", e)))??
    }

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}
