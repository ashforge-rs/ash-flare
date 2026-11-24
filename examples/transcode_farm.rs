//! Media transcoding farm example - distributed video processing

use ash_flare::{RestartPolicy, RestartStrategy, SupervisorHandle, SupervisorSpec, Worker};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;

#[derive(Debug)]
struct TranscodeError(String);

impl std::fmt::Display for TranscodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for TranscodeError {}

#[derive(Debug, Clone)]
struct MediaJob {
    id: usize,
    filename: String,
    format: String,
}

struct TranscodeWorker {
    worker_id: usize,
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<MediaJob>>>,
}

impl TranscodeWorker {
    fn new(worker_id: usize, rx: Arc<tokio::sync::Mutex<mpsc::Receiver<MediaJob>>>) -> Self {
        Self { worker_id, rx }
    }

    async fn transcode(&self, job: MediaJob) -> Result<(), TranscodeError> {
        println!(
            "[Worker {}] Starting: {} -> {}",
            self.worker_id, job.filename, job.format
        );

        let duration = 1000 + (job.id as u64 * 200);
        sleep(Duration::from_millis(duration)).await;

        if job.id == 3 || job.id == 6 {
            println!(
                "[Worker {}] ✗ Failed: {} (corrupted file)",
                self.worker_id, job.filename
            );
            return Err(TranscodeError(format!(
                "Failed to transcode {}: file corrupted",
                job.filename
            )));
        }

        if job.id == 7 {
            println!(
                "[Worker {}] ✗ Failed: {} (unsupported codec)",
                self.worker_id, job.filename
            );
            return Err(TranscodeError(format!(
                "Failed to transcode {}: unsupported codec",
                job.filename
            )));
        }

        println!(
            "[Worker {}] ✓ Completed: {} ({}ms)",
            self.worker_id, job.filename, duration
        );
        Ok(())
    }
}
impl Worker for TranscodeWorker {
    type Error = TranscodeError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("[Worker {}] Ready for transcoding", self.worker_id);
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            let job = {
                let mut rx = self.rx.lock().await;
                rx.recv().await
            };

            match job {
                Some(job) => {
                    self.transcode(job).await?;
                }
                None => {
                    println!("[Worker {}] Queue closed, shutting down", self.worker_id);
                    return Ok(());
                }
            }
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("[Worker {}] Shutting down", self.worker_id);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Media Transcoding Farm ===\n");

    let (tx, rx) = mpsc::channel::<MediaJob>(50);
    let rx = Arc::new(tokio::sync::Mutex::new(rx));

    let num_workers = 4;
    let mut spec =
        SupervisorSpec::new("transcode-farm").with_restart_strategy(RestartStrategy::OneForOne);

    for worker_id in 0..num_workers {
        let rx_clone = Arc::clone(&rx);
        spec = spec.with_worker(
            format!("transcoder-{}", worker_id),
            move || TranscodeWorker::new(worker_id, Arc::clone(&rx_clone)),
            RestartPolicy::Transient,
        );
    }

    let supervisor = SupervisorHandle::start(spec);
    println!("Started farm with {} workers\n", num_workers);

    let jobs = vec![
        MediaJob {
            id: 1,
            filename: "video1.mp4".to_string(),
            format: "720p".to_string(),
        },
        MediaJob {
            id: 2,
            filename: "video2.mov".to_string(),
            format: "1080p".to_string(),
        },
        MediaJob {
            id: 3,
            filename: "video3.avi".to_string(),
            format: "480p".to_string(),
        },
        MediaJob {
            id: 4,
            filename: "video4.mkv".to_string(),
            format: "720p".to_string(),
        },
        MediaJob {
            id: 5,
            filename: "video5.mp4".to_string(),
            format: "1080p".to_string(),
        },
        MediaJob {
            id: 6,
            filename: "video6.mov".to_string(),
            format: "720p".to_string(),
        },
        MediaJob {
            id: 7,
            filename: "video7.mp4".to_string(),
            format: "480p".to_string(),
        },
        MediaJob {
            id: 8,
            filename: "video8.avi".to_string(),
            format: "1080p".to_string(),
        },
    ];

    println!("Submitting {} jobs...\n", jobs.len());
    for job in jobs {
        tx.send(job).await?;
    }

    drop(tx);

    sleep(Duration::from_secs(8)).await;

    println!("\n=== Shutting down farm ===");
    supervisor.shutdown().await?;

    println!("\n=== Farm shutdown complete ===");
    Ok(())
}
