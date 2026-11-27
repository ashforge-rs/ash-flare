use ash_flare::Worker;
use tokio::time::{Duration, sleep};

#[derive(Debug)]
pub struct QuickWorker;
impl Worker for QuickWorker {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        sleep(Duration::from_millis(5)).await;
        Ok(())
    }
}

#[derive(Debug)]
pub struct FailingWorker {
    pub fail_count: u32,
}
impl Worker for FailingWorker {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        self.fail_count += 1;
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "intentional failure",
        ))
    }
}
