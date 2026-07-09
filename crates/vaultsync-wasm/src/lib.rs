#[cfg(target_arch = "wasm32")]
use std::sync::atomic::Ordering;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

#[cfg(target_arch = "wasm32")]
struct ConsoleSubscriber;

#[cfg(target_arch = "wasm32")]
impl tracing::Subscriber for ConsoleSubscriber {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        let level = crate::log::LOG_LEVEL.load(Ordering::Relaxed);
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
        crate::log::raw_console(&format!("[Rust] {}", visitor.message));
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

pub const BUILD_ID: &str = concat!(env!("VAULTSYNC_BUILD_DATE"), "-", env!("VAULTSYNC_GIT_HASH"));

#[macro_export]
macro_rules! engine_trace {
    ($($arg:tt)*) => {
        if $crate::log::enabled($crate::log::LogLevel::Trace) {
            $crate::log::trace(&format!($($arg)*));
        }
    };
}

#[macro_export]
macro_rules! engine_debug {
    ($($arg:tt)*) => {
        if $crate::log::enabled($crate::log::LogLevel::Debug) {
            $crate::log::debug(&format!($($arg)*));
        }
    };
}

#[macro_export]
macro_rules! engine_info {
    ($($arg:tt)*) => {
        if $crate::log::enabled($crate::log::LogLevel::Info) {
            $crate::log::info(&format!($($arg)*));
        }
    };
}

#[macro_export]
macro_rules! engine_warn {
    ($($arg:tt)*) => {
        if $crate::log::enabled($crate::log::LogLevel::Warn) {
            $crate::log::warn(&format!($($arg)*));
        }
    };
}

#[macro_export]
macro_rules! engine_error {
    ($($arg:tt)*) => {
        if $crate::log::enabled($crate::log::LogLevel::Error) {
            $crate::log::error(&format!($($arg)*));
        }
    };
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn set_log_level_from_str(level: &str) {
    crate::log::set_level_from_str(level);
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
pub mod log;
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
pub mod version_chain;
#[cfg(target_arch = "wasm32")]
pub mod transaction;
#[cfg(target_arch = "wasm32")]
pub mod storage;
#[cfg(target_arch = "wasm32")]
pub mod storage_manager;
#[cfg(target_arch = "wasm32")]
pub mod transport;
#[cfg(target_arch = "wasm32")]
pub mod ws_coordinator;
#[cfg(target_arch = "wasm32")]
pub mod scheduler;
#[cfg(target_arch = "wasm32")]
pub mod metrics;
#[cfg(target_arch = "wasm32")]
pub mod memory;
#[cfg(target_arch = "wasm32")]
pub mod runtime;
#[cfg(target_arch = "wasm32")]
pub mod persistence_engine;
#[cfg(target_arch = "wasm32")]
pub mod capability;
#[cfg(target_arch = "wasm32")]
pub mod access_tracker;
#[cfg(target_arch = "wasm32")]
pub mod broadcast_manager;
#[cfg(target_arch = "wasm32")]
pub mod discovery_protocol;
#[cfg(target_arch = "wasm32")]
pub mod runtime_coordinator;
#[cfg(target_arch = "wasm32")]
pub mod prefetch_manager;
#[cfg(target_arch = "wasm32")]
pub mod follower;
#[cfg(target_arch = "wasm32")]
pub mod recovery_manager;
#[cfg(target_arch = "wasm32")]
pub mod upload_scheduler;
#[cfg(target_arch = "wasm32")]
pub mod compaction_scheduler;
#[cfg(target_arch = "wasm32")]
pub mod storage_scheduler;
#[cfg(target_arch = "wasm32")]
pub mod storage_runtime;
#[cfg(target_arch = "wasm32")]
pub mod runtime_host;
#[cfg(target_arch = "wasm32")]
pub mod sync_runtime;
#[cfg(target_arch = "wasm32")]
pub mod document_runtime;

#[cfg(not(target_arch = "wasm32"))]
#[allow(dead_code)]
fn placeholder() {} // no-op: crate is only built by wasm-pack for wasm32
