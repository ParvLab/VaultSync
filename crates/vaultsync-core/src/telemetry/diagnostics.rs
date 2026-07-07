use web_time::Instant;
use tracing::level_filters::LevelFilter;

#[inline]
pub fn log_data<F, T>(level: LevelFilter, f: F) -> Option<T>
where
    F: FnOnce() -> T,
{
    if level <= LevelFilter::current() {
        Some(f())
    } else {
        None
    }
}

pub struct ScopedTimer {
    start: Instant,
}

impl ScopedTimer {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    pub fn elapsed_us(&self) -> u64 {
        self.start.elapsed().as_micros() as u64
    }

    pub fn elapsed_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }
}

impl Default for ScopedTimer {
    fn default() -> Self {
        Self::new()
    }
}
