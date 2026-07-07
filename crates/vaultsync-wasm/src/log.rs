use std::sync::atomic::{AtomicU8, Ordering};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console, js_name = error)]
    fn console_error_str(s: &str);
    #[wasm_bindgen(js_namespace = console, js_name = warn)]
    fn console_warn_str(s: &str);
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn console_info_str(s: &str);
    #[wasm_bindgen(js_namespace = console, js_name = debug)]
    fn console_debug_str(s: &str);
    #[wasm_bindgen(js_namespace = console, js_name = trace)]
    fn console_trace_str(s: &str);
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Off = 0,
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
    Trace = 5,
}

pub(crate) static LOG_LEVEL: AtomicU8 = AtomicU8::new(LogLevel::Info as u8);

pub fn set_level(level: LogLevel) {
    LOG_LEVEL.store(level as u8, Ordering::SeqCst);
}

pub fn set_level_from_str(level: &str) {
    let lvl = match level.to_lowercase().as_str() {
        "off" => LogLevel::Off,
        "error" => LogLevel::Error,
        "warn" => LogLevel::Warn,
        "info" => LogLevel::Info,
        "debug" => LogLevel::Debug,
        "trace" => LogLevel::Trace,
        _ => LogLevel::Info,
    };
    set_level(lvl);
}

pub fn current_level() -> LogLevel {
    match LOG_LEVEL.load(Ordering::Relaxed) {
        0 => LogLevel::Off,
        1 => LogLevel::Error,
        2 => LogLevel::Warn,
        3 => LogLevel::Info,
        4 => LogLevel::Debug,
        _ => LogLevel::Trace,
    }
}

pub fn enabled(level: LogLevel) -> bool {
    level as u8 <= LOG_LEVEL.load(Ordering::Relaxed)
}

fn emit(level: LogLevel, msg: &str) {
    match level {
        LogLevel::Error => console_error_str(msg),
        LogLevel::Warn => console_warn_str(msg),
        LogLevel::Info => console_info_str(msg),
        LogLevel::Debug => console_debug_str(msg),
        LogLevel::Trace => console_trace_str(msg),
        LogLevel::Off => {}
    }
}

#[inline]
pub fn trace(msg: &str) {
    if enabled(LogLevel::Trace) {
        emit(LogLevel::Trace, msg);
    }
}

#[inline]
pub fn debug(msg: &str) {
    if enabled(LogLevel::Debug) {
        emit(LogLevel::Debug, msg);
    }
}

#[inline]
pub fn info(msg: &str) {
    if enabled(LogLevel::Info) {
        emit(LogLevel::Info, msg);
    }
}

#[inline]
pub fn warn(msg: &str) {
    if enabled(LogLevel::Warn) {
        emit(LogLevel::Warn, msg);
    }
}

#[inline]
pub fn error(msg: &str) {
    if enabled(LogLevel::Error) {
        emit(LogLevel::Error, msg);
    }
}

/// Raw console.log accessor for ConsoleSubscriber (tracing integration).
/// Always emits at INFO level — the subscriber handles its own filtering.
pub(crate) fn raw_console(msg: &str) {
    console_info_str(msg);
}


