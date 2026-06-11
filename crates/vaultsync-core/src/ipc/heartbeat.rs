use crate::time_utils::PlatformInstant;
use std::time::Duration;

pub struct Heartbeat {
    last_tick: PlatformInstant,
    _interval: Duration,
}

impl Heartbeat {
    pub fn new(interval: Duration) -> Self {
        Self {
            last_tick: PlatformInstant::now(),
            _interval: interval,
        }
    }
    pub fn tick(&mut self) {
        self.last_tick = PlatformInstant::now();
    }
    pub fn is_stale(&self, timeout: Duration) -> bool {
        self.last_tick.elapsed() > timeout
    }
}
