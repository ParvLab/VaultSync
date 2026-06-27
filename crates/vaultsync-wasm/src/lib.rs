#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn console_log_str(s: &str);
}

#[cfg(target_arch = "wasm32")]
struct ConsoleSubscriber;

#[cfg(target_arch = "wasm32")]
impl tracing::Subscriber for ConsoleSubscriber {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.level() <= &tracing::Level::INFO
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        struct Visitor {
            message: String,
        }
        impl tracing::field::Visit for Visitor {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.message = format!("{:?}", value);
                } else {
                    if !self.message.is_empty() {
                        self.message.push_str(", ");
                    }
                    self.message
                        .push_str(&format!("{}={:?}", field.name(), value));
                }
            }
        }
        let mut visitor = Visitor {
            message: String::new(),
        };
        event.record(&mut visitor);
        console_log_str(&format!("[Rust] {}", visitor.message));
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

pub const BUILD_ID: &str = concat!(env!("VAULTSYNC_BUILD_DATE"), "-", env!("VAULTSYNC_GIT_HASH"));

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
    let _ = tracing::subscriber::set_global_default(ConsoleSubscriber);
    tracing::warn!("================================================");
    tracing::warn!("VaultSync WASM BUILD: {} instrumentation=v3", BUILD_ID);
    tracing::warn!("================================================");
}

#[cfg(target_arch = "wasm32")]
pub mod client;
#[cfg(target_arch = "wasm32")]
pub mod e2ee;
#[cfg(target_arch = "wasm32")]
pub mod indexeddb;
#[cfg(target_arch = "wasm32")]
pub mod ipc;
#[cfg(target_arch = "wasm32")]
pub mod mutation_store;
#[cfg(target_arch = "wasm32")]
pub mod presence;
#[cfg(target_arch = "wasm32")]
pub mod storage;
#[cfg(target_arch = "wasm32")]
pub mod transport;
#[cfg(target_arch = "wasm32")]
pub mod ws_coordinator;

#[cfg(not(target_arch = "wasm32"))]
fn placeholder() {} // no-op: crate is only built by wasm-pack for wasm32
