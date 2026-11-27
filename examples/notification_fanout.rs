//! Notification fanout system - topic-based and campaign-based delivery

use ash_flare::{RestartPolicy, RestartStrategy, SupervisorHandle, SupervisorSpec, Worker};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;

#[derive(Debug)]
struct NotificationError(String);

impl std::fmt::Display for NotificationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for NotificationError {}

#[derive(Debug, Clone)]
enum NotificationType {
    Topic {
        topic: String,
        message: String,
    },
    Campaign {
        campaign_id: String,
        message: String,
        targets: Vec<String>,
    },
}

#[derive(Debug, Clone)]
struct Notification {
    id: usize,
    notification_type: NotificationType,
}

struct NotificationPublisher {
    tx: mpsc::Sender<Notification>,
}

impl NotificationPublisher {
    fn new(tx: mpsc::Sender<Notification>) -> Self {
        Self { tx }
    }

    async fn publish(&self, notif: Notification) -> Result<(), NotificationError> {
        self.tx
            .send(notif)
            .await
            .map_err(|e| NotificationError(format!("Failed to publish: {}", e)))?;
        Ok(())
    }
}
impl Worker for NotificationPublisher {
    type Error = NotificationError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("[Publisher] Starting notification publisher");
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        sleep(Duration::from_millis(500)).await;

        let notifications = vec![
            Notification {
                id: 1,
                notification_type: NotificationType::Topic {
                    topic: "news".to_string(),
                    message: "Breaking: Major update released".to_string(),
                },
            },
            Notification {
                id: 2,
                notification_type: NotificationType::Campaign {
                    campaign_id: "summer-sale".to_string(),
                    message: "Summer Sale: 50% off everything!".to_string(),
                    targets: vec![
                        "user123".to_string(),
                        "user456".to_string(),
                        "user789".to_string(),
                    ],
                },
            },
            Notification {
                id: 3,
                notification_type: NotificationType::Topic {
                    topic: "alerts".to_string(),
                    message: "System maintenance scheduled".to_string(),
                },
            },
            Notification {
                id: 4,
                notification_type: NotificationType::Campaign {
                    campaign_id: "new-feature".to_string(),
                    message: "Try our new AI assistant!".to_string(),
                    targets: vec!["user123".to_string(), "user999".to_string()],
                },
            },
            Notification {
                id: 5,
                notification_type: NotificationType::Topic {
                    topic: "news".to_string(),
                    message: "New content available".to_string(),
                },
            },
        ];

        for notif in notifications {
            println!("\n[Publisher] Publishing notification #{}", notif.id);
            self.publish(notif).await?;
            sleep(Duration::from_millis(800)).await;
        }

        println!("\n[Publisher] All notifications published");
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("[Publisher] Shutting down");
        Ok(())
    }
}

struct FanoutWorker {
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<Notification>>>,
    topic_subscribers: HashMap<String, Vec<String>>,
}

impl FanoutWorker {
    fn new(rx: Arc<tokio::sync::Mutex<mpsc::Receiver<Notification>>>) -> Self {
        let mut topic_subscribers = HashMap::new();
        topic_subscribers.insert(
            "news".to_string(),
            vec![
                "user123".to_string(),
                "user456".to_string(),
                "user789".to_string(),
            ],
        );
        topic_subscribers.insert(
            "alerts".to_string(),
            vec!["user123".to_string(), "user999".to_string()],
        );

        Self {
            rx,
            topic_subscribers,
        }
    }

    async fn fanout_topic(&self, topic: &str, message: &str) -> Result<(), NotificationError> {
        if let Some(subscribers) = self.topic_subscribers.get(topic) {
            println!(
                "  [Fanout] Topic '{}' → {} subscribers",
                topic,
                subscribers.len()
            );
            for subscriber in subscribers {
                sleep(Duration::from_millis(100)).await;
                println!("    ✓ Delivered to {}: {}", subscriber, message);
            }
        } else {
            println!("  [Fanout] Topic '{}' has no subscribers", topic);
        }
        Ok(())
    }

    async fn fanout_campaign(
        &self,
        campaign_id: &str,
        message: &str,
        targets: &[String],
    ) -> Result<(), NotificationError> {
        println!(
            "  [Fanout] Campaign '{}' → {} targets",
            campaign_id,
            targets.len()
        );
        for target in targets {
            sleep(Duration::from_millis(100)).await;

            if target == "user999" && campaign_id == "new-feature" {
                println!("    ✗ Failed to deliver to {}: user offline", target);
                continue;
            }

            println!("    ✓ Delivered to {}: {}", target, message);
        }
        Ok(())
    }
}
impl Worker for FanoutWorker {
    type Error = NotificationError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("[Fanout] Starting notification fanout worker");
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            let notif = {
                let mut rx = self.rx.lock().await;
                rx.recv().await
            };

            match notif {
                Some(notification) => match notification.notification_type {
                    NotificationType::Topic {
                        ref topic,
                        ref message,
                    } => {
                        self.fanout_topic(topic, message).await?;
                    }
                    NotificationType::Campaign {
                        ref campaign_id,
                        ref message,
                        ref targets,
                    } => {
                        self.fanout_campaign(campaign_id, message, targets).await?;
                    }
                },
                None => {
                    println!("[Fanout] Channel closed, shutting down");
                    return Ok(());
                }
            }
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("[Fanout] Shutting down");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Notification Fanout System ===\n");

    let (tx, rx) = mpsc::channel::<Notification>(32);
    let rx = Arc::new(tokio::sync::Mutex::new(rx));

    let spec = SupervisorSpec::new("notification-system")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "publisher",
            move || NotificationPublisher::new(tx.clone()),
            RestartPolicy::Transient,
        )
        // Transient: once the publisher is done and the channel closes, the
        // fanout finishes normally and is not restarted. As Permanent it would
        // be restarted onto the closed channel until the supervisor gave up.
        .with_worker(
            "fanout",
            move || FanoutWorker::new(Arc::clone(&rx)),
            RestartPolicy::Transient,
        );

    let supervisor = SupervisorHandle::start(spec).expect("valid supervisor spec");

    println!("Started notification system\n");

    sleep(Duration::from_secs(8)).await;

    println!("\n=== Shutting down system ===");
    supervisor.shutdown().await?;

    println!("\n=== System shutdown complete ===");
    Ok(())
}
