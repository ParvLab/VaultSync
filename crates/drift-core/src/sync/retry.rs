use std::time::Duration;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub backoff_factor: f64,
    pub jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 10,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(120),
            backoff_factor: 2.0,
            jitter: true,
        }
    }
}

pub struct RetryEngine {
    config: RetryConfig,
    attempts: HashMap<String, u32>,
}

impl RetryEngine {
    pub fn new(config: RetryConfig) -> Self {
        Self { config, attempts: HashMap::new() }
    }

    pub fn record_failure(&mut self, id: &str) -> Option<Duration> {
        let attempts = self.attempts.entry(id.to_string()).or_insert(0);
        *attempts += 1;
        if *attempts >= self.config.max_attempts {
            return None;
        }
        let delay = self.config.initial_delay.as_secs_f64()
            * self.config.backoff_factor.powi(*attempts as i32 - 1);
        let delay = delay.min(self.config.max_delay.as_secs_f64());
        let mut delay = Duration::from_secs_f64(delay);
        if self.config.jitter {
            let jitter = rand::random::<f64>() * 0.5 + 0.75;
            delay = Duration::from_secs_f64(delay.as_secs_f64() * jitter);
        }
        Some(delay)
    }

    pub fn record_success(&mut self, id: &str) {
        self.attempts.remove(id);
    }

    pub fn is_exhausted(&self, id: &str) -> bool {
        self.attempts.get(id).copied().unwrap_or(0) >= self.config.max_attempts
    }
}
