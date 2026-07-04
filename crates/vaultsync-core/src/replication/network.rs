use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NetworkTier {
    Offline,
    Cellular,
    WiFi,
    Ethernet,
}

pub struct NetworkMonitor {
    current_tier: NetworkTier,
    latency_ms: Option<u64>,
    packet_loss_rate: f64,
}

impl NetworkMonitor {
    pub fn new() -> Self {
        Self {
            current_tier: NetworkTier::WiFi,
            latency_ms: None,
            packet_loss_rate: 0.0,
        }
    }

    pub fn set_tier(&mut self, tier: NetworkTier) {
        self.current_tier = tier;
    }

    pub fn current_tier(&self) -> NetworkTier {
        self.current_tier
    }

    pub fn set_latency(&mut self, ms: u64) {
        self.latency_ms = Some(ms);
    }

    pub fn latency_ms(&self) -> Option<u64> {
        self.latency_ms
    }

    pub fn set_packet_loss(&mut self, rate: f64) {
        self.packet_loss_rate = rate.clamp(0.0, 1.0);
    }

    pub fn packet_loss_rate(&self) -> f64 {
        self.packet_loss_rate
    }

    pub fn is_reliable(&self) -> bool {
        self.packet_loss_rate < 0.05 && self.latency_ms.map_or(true, |l| l < 200)
    }
}

impl Default for NetworkMonitor {
    fn default() -> Self {
        Self::new()
    }
}

pub struct BandwidthEstimator {
    samples: VecDeque<(u64, u64)>,
    max_samples: usize,
}

impl BandwidthEstimator {
    pub fn new() -> Self {
        Self {
            samples: VecDeque::new(),
            max_samples: 10,
        }
    }

    pub fn record_sample(&mut self, bytes: u64, duration_ms: u64) {
        if duration_ms > 0 {
            self.samples.push_back((bytes, duration_ms));
            if self.samples.len() > self.max_samples {
                self.samples.pop_front();
            }
        }
    }

    pub fn estimated_bps(&self) -> u64 {
        if self.samples.is_empty() {
            return 1_000_000;
        }

        let total_bytes: u64 = self.samples.iter().map(|(b, _)| b).sum();
        let total_ms: u64 = self.samples.iter().map(|(_, d)| d).sum();

        if total_ms > 0 {
            (total_bytes * 1000) / total_ms
        } else {
            1_000_000
        }
    }

    pub fn samples(&self) -> &VecDeque<(u64, u64)> {
        &self.samples
    }

    pub fn clear(&mut self) {
        self.samples.clear();
    }
}

impl Default for BandwidthEstimator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_network_monitor_default() {
        let monitor = NetworkMonitor::new();
        assert_eq!(monitor.current_tier(), NetworkTier::WiFi);
        assert!(monitor.is_reliable());
    }

    #[test]
    fn test_network_monitor_set_tier() {
        let mut monitor = NetworkMonitor::new();
        monitor.set_tier(NetworkTier::Cellular);
        assert_eq!(monitor.current_tier(), NetworkTier::Cellular);
    }

    #[test]
    fn test_network_monitor_latency() {
        let mut monitor = NetworkMonitor::new();
        assert!(monitor.latency_ms().is_none());

        monitor.set_latency(50);
        assert_eq!(monitor.latency_ms(), Some(50));
    }

    #[test]
    fn test_network_monitor_packet_loss() {
        let mut monitor = NetworkMonitor::new();
        assert_eq!(monitor.packet_loss_rate(), 0.0);

        monitor.set_packet_loss(0.1);
        assert_eq!(monitor.packet_loss_rate(), 0.1);
    }

    #[test]
    fn test_network_monitor_reliability() {
        let mut monitor = NetworkMonitor::new();
        monitor.set_latency(100);
        monitor.set_packet_loss(0.01);
        assert!(monitor.is_reliable());

        monitor.set_packet_loss(0.1);
        assert!(!monitor.is_reliable());
    }

    #[test]
    fn test_bandwidth_estimator() {
        let mut estimator = BandwidthEstimator::new();
        assert_eq!(estimator.estimated_bps(), 1_000_000);

        estimator.record_sample(1_000_000, 1000);
        assert_eq!(estimator.estimated_bps(), 1_000_000);

        estimator.record_sample(2_000_000, 1000);
        assert_eq!(estimator.estimated_bps(), 1_500_000);
    }

    #[test]
    fn test_bandwidth_estimator_max_samples() {
        let mut estimator = BandwidthEstimator::new();
        for i in 0..20 {
            estimator.record_sample(1000, 100);
        }
        assert_eq!(estimator.samples().len(), 10);
    }

    #[test]
    fn test_bandwidth_estimator_clear() {
        let mut estimator = BandwidthEstimator::new();
        estimator.record_sample(1_000_000, 1000);
        assert!(!estimator.samples().is_empty());

        estimator.clear();
        assert!(estimator.samples().is_empty());
    }
}
