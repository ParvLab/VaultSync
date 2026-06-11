#[cfg(feature = "telemetry")]
use serde_json::json;
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock};

#[derive(Clone, Default)]
pub struct SpanBuffer {
    buffer: Arc<Mutex<VecDeque<Value>>>,
}

impl SpanBuffer {
    pub fn new() -> Self {
        Self {
            buffer: Arc::new(Mutex::new(VecDeque::with_capacity(1000))),
        }
    }

    pub fn push(&self, span: Value) {
        let mut buf = self.buffer.lock().unwrap();
        if buf.len() >= 1000 {
            buf.pop_front();
        }
        buf.push_back(span);
    }

    pub fn get_all(&self) -> Vec<Value> {
        let buf = self.buffer.lock().unwrap();
        buf.iter().cloned().collect()
    }
}

static GLOBAL_SPAN_BUFFER: OnceLock<SpanBuffer> = OnceLock::new();

pub fn get_span_buffer() -> &'static SpanBuffer {
    GLOBAL_SPAN_BUFFER.get_or_init(SpanBuffer::new)
}

#[cfg(feature = "telemetry")]
use tracing_subscriber::{prelude::*, EnvFilter, Registry};

#[cfg(feature = "telemetry")]
struct FieldVisitor<'a>(&'a mut std::collections::HashMap<String, Value>);

#[cfg(feature = "telemetry")]
impl<'a> tracing::field::Visit for FieldVisitor<'a> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), json!(format!("{:?}", value)));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().to_string(), json!(value));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.0.insert(field.name().to_string(), json!(value));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0.insert(field.name().to_string(), json!(value));
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.0.insert(field.name().to_string(), json!(value));
    }
}

#[cfg(feature = "telemetry")]
pub struct SpanCollectorLayer;

#[cfg(feature = "telemetry")]
impl<S> tracing_subscriber::Layer<S> for SpanCollectorLayer
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if let Some(span) = ctx.span(id) {
            let mut fields = std::collections::HashMap::new();
            let mut visitor = FieldVisitor(&mut fields);
            attrs.record(&mut visitor);

            let val = json!({
                "id": id.into_u64(),
                "name": span.name(),
                "fields": fields,
                "timestamp": chrono::Utc::now().to_rfc3339(),
            });
            get_span_buffer().push(val);
        }
    }
}

#[cfg(feature = "telemetry")]
pub fn init_tracing(service_name: &str, otlp_endpoint: Option<&str>) {
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::{
        trace::{self, Sampler},
        Resource,
    };

    let endpoint = otlp_endpoint
        .map(|s| s.to_string())
        .or_else(|| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok());

    let filter = EnvFilter::from_default_env()
        .add_directive("vaultsync_core=info".parse().unwrap())
        .add_directive("vaultsync=info".parse().unwrap());

    if let Some(ep) = endpoint {
        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(opentelemetry_otlp::new_exporter().tonic().with_endpoint(ep))
            .with_trace_config(
                trace::config()
                    .with_sampler(Sampler::AlwaysOn)
                    .with_resource(Resource::new(vec![KeyValue::new(
                        "service.name",
                        service_name.to_string(),
                    )])),
            )
            .install_batch(opentelemetry_sdk::runtime::Tokio);

        match tracer {
            Ok(tracer) => {
                let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);
                let subscriber = Registry::default()
                    .with(filter)
                    .with(SpanCollectorLayer)
                    .with(telemetry_layer);
                let _ = tracing::subscriber::set_global_default(subscriber);
                return;
            }
            Err(err) => {
                eprintln!(
                    "Failed to initialize OTLP tracer: {}, falling back to JSON stdout",
                    err
                );
            }
        }
    }

    // Fallback to JSON stdout formatter
    let subscriber = Registry::default()
        .with(filter)
        .with(SpanCollectorLayer)
        .with(tracing_subscriber::fmt::layer().json());
    let _ = tracing::subscriber::set_global_default(subscriber);
}

#[cfg(not(feature = "telemetry"))]
pub fn init_tracing(_service_name: &str, _otlp_endpoint: Option<&str>) {
    // no-op
}

// Keep the pub struct VaultSyncTelemetry for backward compatibility if any parts of the code use it
pub struct VaultSyncTelemetry;

impl VaultSyncTelemetry {
    pub fn new() -> Self {
        Self
    }
}
