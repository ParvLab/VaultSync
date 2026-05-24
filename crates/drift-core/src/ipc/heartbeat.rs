use std::time::{Duration, Instant};

pub struct Heartbeat {
    last_tick: Instant,
    _interval: Duration,
}

impl Heartbeat {
    pub fn new(interval: Duration) -> Self { Self { last_tick: Instant::now(), _interval: interval } }
    pub fn tick(&mut self) { self.last_tick = Instant::now(); }
    pub fn is_stale(&self, timeout: Duration) -> bool { self.last_tick.elapsed() > timeout }
}
