use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn console_log_str(s: &str);
}

struct ConsoleSubscriber;

impl tracing::Subscriber for ConsoleSubscriber {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
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
                    self.message.push_str(&format!("{}={:?}", field.name(), value));
                }
            }
        }
        let mut visitor = Visitor { message: String::new() };
        event.record(&mut visitor);
        console_log_str(&format!("[Rust] {}", visitor.message));
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
    let _ = tracing::subscriber::set_global_default(ConsoleSubscriber);
}

pub mod client;
pub mod storage;
pub mod indexeddb;
pub mod ipc;
pub mod e2ee;
pub mod transport;
pub mod ws_coordinator;
