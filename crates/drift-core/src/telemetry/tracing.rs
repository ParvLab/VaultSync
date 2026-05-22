pub struct DriftTelemetry;

impl DriftTelemetry {
    pub fn new() -> Self {
        Self
    }
}

#[cfg(feature = "telemetry")]
pub fn init() {
    use tracing_subscriber::EnvFilter;
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env()
            .add_directive("drift_core=info".parse().unwrap())
            .add_directive("drift=info".parse().unwrap()))
        .json()
        .init();
}

#[cfg(not(feature = "telemetry"))]
pub fn init() {
    // no-op without telemetry feature
}
