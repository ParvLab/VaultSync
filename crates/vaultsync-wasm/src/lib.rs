#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn console_log_str(s: &str);
}

#[cfg(target_arch = "wasm32")]
use std::sync::atomic::{AtomicU8, Ordering};

#[cfg(target_arch = "wasm32")]
static LOG_LEVEL: AtomicU8 = AtomicU8::new(3); // 0=Off, 1=Error, 2=Warn, 3=Info, 4=Debug, 5=Trace

#[cfg(target_arch = "wasm32")]
pub fn set_log_level(level: u8) {
    LOG_LEVEL.store(level.min(5), Ordering::SeqCst);
}

#[cfg(target_arch = "wasm32")]
struct ConsoleSubscriber;

#[cfg(target_arch = "wasm32")]
impl tracing::Subscriber for ConsoleSubscriber {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        let level = LOG_LEVEL.load(Ordering::Relaxed);
        if level == 0 { return false; }
        let event_level = match *metadata.level() {
            tracing::Level::ERROR => 1,
            tracing::Level::WARN => 2,
            tracing::Level::INFO => 3,
            tracing::Level::DEBUG => 4,
            tracing::Level::TRACE => 5,
        };
        event_level <= level
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
#[wasm_bindgen]
pub fn set_log_level_from_str(level: &str) {
    let lvl = match level.to_lowercase().as_str() {
        "off" => 0,
        "error" => 1,
        "warn" => 2,
        "info" => 3,
        "debug" => 4,
        "trace" => 5,
        _ => 3,
    };
    set_log_level(lvl);
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
    let _ = tracing::subscriber::set_global_default(ConsoleSubscriber);
    tracing::warn!("================================================");
    tracing::warn!("VaultSync WASM BUILD: {} instrumentation=v6", BUILD_ID);
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
pub mod migration;
#[cfg(target_arch = "wasm32")]
pub mod page_store;
#[cfg(target_arch = "wasm32")]
pub mod storage;
#[cfg(target_arch = "wasm32")]
pub mod storage_manager;
#[cfg(target_arch = "wasm32")]
pub mod transport;
#[cfg(target_arch = "wasm32")]
pub mod ws_coordinator;

#[cfg(not(target_arch = "wasm32"))]
#[allow(dead_code)]
fn placeholder() {} // no-op: crate is only built by wasm-pack for wasm32
