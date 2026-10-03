use std::time::Duration;

/// Tracks the elapsed time for a Raft election timeout.
///
/// This component deliberately does not own a real clock.
/// The caller advances it by providing elapsed durations.
///
/// Keeping the timer deterministic makes it much easier to test
/// election behavior and simulate failures later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ElectionTimeout {
    timeout: Duration,
    elapsed: Duration,
}

impl ElectionTimeout {
    /// Creates a new election timeout.
    ///
    /// # Panics
    ///
    /// Panics if `timeout` is zero because a zero-length election
    /// timeout would cause elections to trigger immediately.
    pub fn new(timeout: Duration) -> Self {
        assert!(
            !timeout.is_zero(),
            "election timeout must be greater than zero"
        );

        Self {
            timeout,
            elapsed: Duration::ZERO,
        }
    }

    /// Returns the configured election timeout.
    pub const fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Returns the amount of time accumulated since the last reset.
    pub const fn elapsed(&self) -> Duration {
        self.elapsed
    }

    /// Returns the remaining time before the election timeout expires.
    pub fn remaining(&self) -> Duration {
        self.timeout.saturating_sub(self.elapsed)
    }

    /// Returns `true` when the election timeout has expired.
    pub fn expired(&self) -> bool {
        self.elapsed >= self.timeout
    }

    /// Advances the timer by the supplied duration.
    ///
    /// The elapsed duration is capped at the configured timeout.
    /// This prevents the timer from growing without bound.
    pub fn advance(&mut self, duration: Duration) {
        self.elapsed = self.elapsed.saturating_add(duration).min(self.timeout);
    }

    /// Resets the election timer.
    ///
    /// This is called when the node receives valid leader activity
    /// or when a new election begins.
    pub fn reset(&mut self) {
        self.elapsed = Duration::ZERO;
    }
}

#[cfg(test)]
mod tests {
    use super::ElectionTimeout;
    use std::time::Duration;

    #[test]
    fn new_timer_starts_at_zero() {
        let timer = ElectionTimeout::new(Duration::from_millis(500));

        assert_eq!(timer.elapsed(), Duration::ZERO);
    }

    #[test]
    fn timer_is_not_expired_when_created() {
        let timer = ElectionTimeout::new(Duration::from_millis(500));

        assert!(!timer.expired());
    }

    #[test]
    fn timer_expires_after_timeout() {
        let mut timer = ElectionTimeout::new(Duration::from_millis(500));

        timer.advance(Duration::from_millis(500));

        assert!(timer.expired());
    }

    #[test]
    fn timer_does_not_expire_before_timeout() {
        let mut timer = ElectionTimeout::new(Duration::from_millis(500));

        timer.advance(Duration::from_millis(499));

        assert!(!timer.expired());
    }

    #[test]
    fn elapsed_time_is_tracked() {
        let mut timer = ElectionTimeout::new(Duration::from_millis(500));

        timer.advance(Duration::from_millis(200));

        assert_eq!(timer.elapsed(), Duration::from_millis(200));
    }

    #[test]
    fn remaining_time_is_calculated() {
        let mut timer = ElectionTimeout::new(Duration::from_millis(500));

        timer.advance(Duration::from_millis(200));

        assert_eq!(timer.remaining(), Duration::from_millis(300));
    }

    #[test]
    fn reset_clears_elapsed_time() {
        let mut timer = ElectionTimeout::new(Duration::from_millis(500));

        timer.advance(Duration::from_millis(400));
        timer.reset();

        assert_eq!(timer.elapsed(), Duration::ZERO);
        assert!(!timer.expired());
    }

    #[test]
    fn elapsed_time_is_capped_at_timeout() {
        let mut timer = ElectionTimeout::new(Duration::from_millis(500));

        timer.advance(Duration::from_secs(10));

        assert_eq!(timer.elapsed(), Duration::from_millis(500));
        assert!(timer.expired());
    }

    #[test]
    #[should_panic(expected = "election timeout must be greater than zero")]
    fn zero_timeout_is_rejected() {
        ElectionTimeout::new(Duration::ZERO);
    }
}
