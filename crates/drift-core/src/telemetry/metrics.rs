pub struct DriftMetrics;

impl DriftMetrics {
    pub fn new() -> Self { Self }
    pub fn record_upload(&self, _count: usize) {}
    pub fn record_download(&self, _count: usize) {}
    pub fn record_sync_lag(&self, _ms: f64) {}
}
