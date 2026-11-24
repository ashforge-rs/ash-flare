//! Telemetry ingestion and processing example - IoT data pipeline

use ash_flare::{RestartPolicy, RestartStrategy, SupervisorHandle, SupervisorSpec, Worker};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{interval, sleep};

#[derive(Debug)]
struct TelemetryError(String);

impl std::fmt::Display for TelemetryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for TelemetryError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TelemetryData {
    device_id: u32,
    payload: f64,
}

struct TelemetryIngester {
    tx: mpsc::Sender<TelemetryData>,
}

impl TelemetryIngester {
    fn new(tx: mpsc::Sender<TelemetryData>) -> Self {
        Self { tx }
    }

    async fn simulate_device_data(&self, device_id: u32) -> Result<(), TelemetryError> {
        let data = TelemetryData {
            device_id,
            payload: rand::random::<f64>() * 100.0,
        };

        self.tx
            .send(data.clone())
            .await
            .map_err(|e| TelemetryError(format!("Failed to send telemetry: {}", e)))?;

        println!("[Ingester] Device {} -> {:.2}", device_id, data.payload);
        Ok(())
    }
}
impl Worker for TelemetryIngester {
    type Error = TelemetryError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("[Ingester] Starting telemetry ingestion");
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        let mut tick = interval(Duration::from_millis(500));
        let mut counter = 0u32;

        loop {
            tick.tick().await;

            let device_id = (counter % 5) + 1;
            self.simulate_device_data(device_id).await?;

            counter += 1;
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("[Ingester] Shutting down");
        Ok(())
    }
}

struct TelemetryProcessor {
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<TelemetryData>>>,
    buffer: HashMap<u32, Vec<f64>>,
    window_secs: u64,
}

impl TelemetryProcessor {
    fn new(rx: Arc<tokio::sync::Mutex<mpsc::Receiver<TelemetryData>>>, window_secs: u64) -> Self {
        Self {
            rx,
            buffer: HashMap::new(),
            window_secs,
        }
    }

    fn add_reading(&mut self, data: TelemetryData) {
        self.buffer
            .entry(data.device_id)
            .or_insert_with(Vec::new)
            .push(data.payload);
    }

    fn compute_and_output_averages(&mut self) {
        if self.buffer.is_empty() {
            return;
        }

        println!("\n=== Telemetry Report ({}s window) ===", self.window_secs);

        for (device_id, readings) in self.buffer.iter() {
            if readings.is_empty() {
                continue;
            }

            let sum: f64 = readings.iter().sum();
            let avg = sum / readings.len() as f64;

            println!(
                "Device {}: avg={:.2} (samples={})",
                device_id,
                avg,
                readings.len()
            );
        }

        println!();
        self.buffer.clear();
    }
}
impl Worker for TelemetryProcessor {
    type Error = TelemetryError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("[Processor] Starting telemetry processing");
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        let mut output_timer = interval(Duration::from_secs(self.window_secs));

        loop {
            tokio::select! {
                _ = output_timer.tick() => {
                    self.compute_and_output_averages();
                }

                data = async {
                    let mut rx = self.rx.lock().await;
                    rx.recv().await
                } => {
                    match data {
                        Some(telemetry) => {
                            self.add_reading(telemetry);
                        }
                        None => {
                            println!("[Processor] Channel closed, shutting down");
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("[Processor] Shutting down");
        self.compute_and_output_averages();
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Telemetry Ingestion & Processing Example ===\n");
    println!("Simulating IoT devices sending telemetry data");
    println!("Data format: {{\"device_id\": N, \"payload\": X.X}}\n");

    let (tx, rx) = mpsc::channel::<TelemetryData>(100);
    let rx = Arc::new(tokio::sync::Mutex::new(rx));

    let spec = SupervisorSpec::new("telemetry-supervisor")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "ingester",
            move || TelemetryIngester::new(tx.clone()),
            RestartPolicy::Permanent,
        )
        .with_worker(
            "processor",
            move || TelemetryProcessor::new(Arc::clone(&rx), 10),
            RestartPolicy::Permanent,
        );

    let supervisor = SupervisorHandle::start(spec).expect("valid supervisor spec");

    println!("Started telemetry pipeline\n");

    sleep(Duration::from_secs(35)).await;

    println!("\n=== Shutting down supervisor ===");
    supervisor.shutdown().await?;

    println!("\n=== Example completed ===");
    Ok(())
}
