use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeTimer {
    interval: Duration,
    elapsed: Duration,
}

impl NodeTimer {
    pub fn new(interval: Duration) -> Self {
        assert!(
            !interval.is_zero(),
            "node timer interval must be greater than zero"
        );

        Self {
            interval,
            elapsed: Duration::ZERO,
        }
    }

    pub const fn interval(&self) -> Duration {
        self.interval
    }

    pub const fn elapsed(&self) -> Duration {
        self.elapsed
    }

    pub fn advance(&mut self, duration: Duration) {
        self.elapsed = self.elapsed.saturating_add(duration);
    }

    pub fn expired(&self) -> bool {
        self.elapsed >= self.interval
    }

    pub fn reset(&mut self) {
        self.elapsed = Duration::ZERO;
    }

    pub fn take_if_expired(&mut self) -> bool {
        if !self.expired() {
            return false;
        }

        self.reset();
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_timer_starts_with_zero_elapsed_time() {
        let timer = NodeTimer::new(Duration::from_millis(100));

        assert_eq!(timer.elapsed(), Duration::ZERO);
        assert_eq!(timer.interval(), Duration::from_millis(100));
    }

    #[test]
    fn timer_does_not_expire_before_interval() {
        let mut timer = NodeTimer::new(Duration::from_millis(100));

        timer.advance(Duration::from_millis(99));

        assert!(!timer.expired());
    }

    #[test]
    fn timer_expires_at_interval() {
        let mut timer = NodeTimer::new(Duration::from_millis(100));

        timer.advance(Duration::from_millis(100));

        assert!(timer.expired());
    }

    #[test]
    fn timer_can_be_reset() {
        let mut timer = NodeTimer::new(Duration::from_millis(100));

        timer.advance(Duration::from_millis(100));
        timer.reset();

        assert_eq!(timer.elapsed(), Duration::ZERO);
        assert!(!timer.expired());
    }

    #[test]
    fn take_if_expired_resets_expired_timer() {
        let mut timer = NodeTimer::new(Duration::from_millis(100));

        timer.advance(Duration::from_millis(100));

        assert!(timer.take_if_expired());
        assert_eq!(timer.elapsed(), Duration::ZERO);
        assert!(!timer.expired());
    }

    #[test]
    fn take_if_expired_returns_false_when_not_expired() {
        let mut timer = NodeTimer::new(Duration::from_millis(100));

        timer.advance(Duration::from_millis(50));

        assert!(!timer.take_if_expired());
        assert_eq!(timer.elapsed(), Duration::from_millis(50));
    }

    #[test]
    #[should_panic(expected = "node timer interval must be greater than zero")]
    fn zero_interval_is_rejected() {
        NodeTimer::new(Duration::ZERO);
    }
}
