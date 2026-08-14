//! Restart policies and strategies for supervision

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

/// Restart strategy for supervisor children
#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[rkyv(derive(Debug))]
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
#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[rkyv(derive(Debug))]
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
    /// Creates a new `RestartIntensity` with the specified limits.
    ///
    /// # Examples
    /// ```
    /// use ash_flare::RestartIntensity;
    /// let intensity = RestartIntensity::new(5, 10);
    /// assert_eq!(intensity.max_restarts, 5);
    /// assert_eq!(intensity.within_seconds, 10);
    /// ```
    #[inline]
    #[must_use]
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

/// Delay applied between a child terminating and being restarted.
///
/// Without a backoff a child that fails immediately is respawned in a tight
/// loop, saturating a CPU core until the restart intensity limit is reached.
/// The delay grows exponentially with the number of consecutive restarts and is
/// capped at `max_delay`.
///
/// # Examples
/// ```
/// use ash_flare::RestartBackoff;
/// use std::time::Duration;
///
/// // No delay between restarts (legacy behaviour).
/// let none = RestartBackoff::none();
/// assert_eq!(none.delay_for_attempt(3), Duration::ZERO);
///
/// // 100ms, doubling up to a 30s ceiling.
/// let backoff = RestartBackoff::exponential(Duration::from_millis(100), Duration::from_secs(30));
/// assert_eq!(backoff.delay_for_attempt(0), Duration::from_millis(100));
/// assert_eq!(backoff.delay_for_attempt(1), Duration::from_millis(200));
/// assert_eq!(backoff.delay_for_attempt(2), Duration::from_millis(400));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestartBackoff {
    /// Delay applied before the first restart.
    pub initial_delay: Duration,
    /// Upper bound on the delay, regardless of how many restarts have occurred.
    pub max_delay: Duration,
}

impl RestartBackoff {
    /// Creates an exponential backoff starting at `initial_delay` and doubling
    /// on each consecutive restart, never exceeding `max_delay`.
    #[inline]
    #[must_use]
    pub const fn exponential(initial_delay: Duration, max_delay: Duration) -> Self {
        Self {
            initial_delay,
            max_delay,
        }
    }

    /// Creates a backoff that never delays, restoring the immediate-restart
    /// behaviour. Prefer [`RestartBackoff::exponential`] for crash-looping work.
    #[inline]
    #[must_use]
    pub const fn none() -> Self {
        Self {
            initial_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
        }
    }

    /// Returns the delay to apply before restart number `attempt` (0-indexed).
    ///
    /// The delay doubles per attempt and saturates at `max_delay`, so large
    /// attempt counts cannot overflow.
    #[must_use]
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        if self.initial_delay.is_zero() {
            return Duration::ZERO;
        }

        // Saturating shift: anything past 2^63 nanos is far beyond max_delay.
        let multiplier = 1_u64.checked_shl(attempt).unwrap_or(u64::MAX);
        let delay = self
            .initial_delay
            .checked_mul(u32::try_from(multiplier).unwrap_or(u32::MAX))
            .unwrap_or(self.max_delay);

        delay.min(self.max_delay)
    }
}

impl Default for RestartBackoff {
    /// Defaults to 100ms doubling up to 30s, which keeps a crash-looping child
    /// from consuming a CPU core.
    fn default() -> Self {
        Self::exponential(Duration::from_millis(100), Duration::from_secs(30))
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
            restart_times: VecDeque::with_capacity(intensity.max_restarts.saturating_add(1)),
        }
    }

    /// Records a restart and returns true if intensity limit exceeded
    pub(crate) fn record_restart(&mut self) -> bool {
        let now = Instant::now();
        let cutoff = now
            .checked_sub(Duration::from_secs(self.intensity.within_seconds))
            .unwrap_or(now);

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

/// Tracks consecutive restarts per child so backoff grows for a child that
/// keeps failing and resets once it has run long enough to be considered healthy.
#[derive(Debug)]
pub(crate) struct BackoffTracker {
    backoff: RestartBackoff,
    /// Consecutive restart count keyed by child id.
    attempts: std::collections::HashMap<String, u32>,
}

impl BackoffTracker {
    pub(crate) fn new(backoff: RestartBackoff) -> Self {
        Self {
            backoff,
            attempts: std::collections::HashMap::new(),
        }
    }

    /// Records a restart for `id` and returns the delay to wait beforehand.
    pub(crate) fn next_delay(&mut self, id: &str) -> Duration {
        let attempt = self.attempts.entry(id.to_owned()).or_insert(0);
        let delay = self.backoff.delay_for_attempt(*attempt);
        *attempt = attempt.saturating_add(1);
        delay
    }

    /// Clears the consecutive-restart count for `id`, so the next failure starts
    /// from the initial delay again.
    pub(crate) fn reset_child(&mut self, id: &str) {
        self.attempts.remove(id);
    }
}
