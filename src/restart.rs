//! Restart policies and strategies for supervision

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

use bincode::{Decode, Encode};

/// Restart strategy for supervisor children
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub enum RestartStrategy {
    /// Restart only the failed child (`:one_for_one`)
    #[default]
    OneForOne,
    /// Restart all children if any child fails (`:one_for_all`)
    OneForAll,
    /// Restart failed child and all children started after it (`:rest_for_one`)
    RestForOne,
}



/// When to restart a child
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub enum RestartPolicy {
    /// Always restart when child terminates (`:permanent`)
    #[default]
    Permanent,
    /// Never restart (`:temporary`)
    Temporary,
    /// Restart only if abnormal termination (`:transient`)
    Transient,
}



/// Restart intensity limits with max restarts within a time window
#[derive(Debug, Clone, Copy)]
pub struct RestartIntensity {
    /// Maximum number of restarts allowed
    pub max_restarts: usize,
    /// Within this time period (in seconds)
    pub within_seconds: u64,
}

impl RestartIntensity {
    /// Creates a new RestartIntensity with the specified limits.
    ///
    /// # Examples
    /// ```
    /// use ash_flare::RestartIntensity;
    /// let intensity = RestartIntensity::new(5, 10);
    /// assert_eq!(intensity.max_restarts, 5);
    /// assert_eq!(intensity.within_seconds, 10);
    /// ```
    #[inline]
    pub const fn new(max_restarts: usize, within_seconds: u64) -> Self {
        Self {
            max_restarts,
            within_seconds,
        }
    }
}

impl Default for RestartIntensity {
    fn default() -> Self {
        Self::new(3, 5)
    }
}

/// Tracks restart history for intensity monitoring using a sliding time window
#[derive(Debug)]
pub(crate) struct RestartTracker {
    intensity: RestartIntensity,
    restart_times: VecDeque<Instant>,
}

impl RestartTracker {
    pub(crate) fn new(intensity: RestartIntensity) -> Self {
        Self {
            intensity,
            // Pre-allocate with max_restarts + 1 to avoid reallocations
            restart_times: VecDeque::with_capacity(intensity.max_restarts + 1),
        }
    }

    /// Records a restart and returns true if intensity limit exceeded
    pub(crate) fn record_restart(&mut self) -> bool {
        let now = Instant::now();
        let cutoff = now - Duration::from_secs(self.intensity.within_seconds);

        // Remove old restarts outside the time window
        while let Some(&time) = self.restart_times.front() {
            if time < cutoff {
                self.restart_times.pop_front();
            } else {
                break;
            }
        }

        self.restart_times.push_back(now);

        // Check if we've exceeded the limit
        self.restart_times.len() > self.intensity.max_restarts
    }

    #[allow(dead_code)]
    pub(crate) fn reset(&mut self) {
        self.restart_times.clear();
    }
}
