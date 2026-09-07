use crate::config::LogLevel;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub trait Logger: Send + Sync {
    fn log(&self, level: LogLevel, module: &str, message: &str);
    fn log_with_fields(&self, level: LogLevel, module: &str, message: &str, fields: &[(&str, &str)]);

    fn error(&self, module: &str, message: &str) {
        self.log(LogLevel::Error, module, message);
    }
    fn warn(&self, module: &str, message: &str) {
        self.log(LogLevel::Warn, module, message);
    }
    fn info(&self, module: &str, message: &str) {
        self.log(LogLevel::Info, module, message);
    }
    fn debug(&self, module: &str, message: &str) {
        self.log(LogLevel::Debug, module, message);
    }
    fn trace(&self, module: &str, message: &str) {
        self.log(LogLevel::Trace, module, message);
    }

    fn error_with(&self, module: &str, message: &str, fields: &[(&str, &str)]) {
        self.log_with_fields(LogLevel::Error, module, message, fields);
    }
    fn warn_with(&self, module: &str, message: &str, fields: &[(&str, &str)]) {
        self.log_with_fields(LogLevel::Warn, module, message, fields);
    }
    fn info_with(&self, module: &str, message: &str, fields: &[(&str, &str)]) {
        self.log_with_fields(LogLevel::Info, module, message, fields);
    }
    fn debug_with(&self, module: &str, message: &str, fields: &[(&str, &str)]) {
        self.log_with_fields(LogLevel::Debug, module, message, fields);
    }
    fn trace_with(&self, module: &str, message: &str, fields: &[(&str, &str)]) {
        self.log_with_fields(LogLevel::Trace, module, message, fields);
    }
}

pub struct NoopLogger;

impl Logger for NoopLogger {
    fn log(&self, _level: LogLevel, _module: &str, _message: &str) {}
    fn log_with_fields(&self, _level: LogLevel, _module: &str, _message: &str, _fields: &[(&str, &str)]) {}
}

pub struct TracingLogger {
    module_config: Option<Arc<ModuleLogConfig>>,
}

impl TracingLogger {
    pub fn new() -> Self {
        Self { module_config: None }
    }

    pub fn with_config(config: Arc<ModuleLogConfig>) -> Self {
        Self { module_config: Some(config) }
    }
}

impl Logger for TracingLogger {
    fn log(&self, level: LogLevel, module: &str, message: &str) {
        if let Some(ref config) = self.module_config {
            if !config.allows(module, &level) {
                return;
            }
        }
        let msg = format!("[{}] {}", module, message);
        match level {
            LogLevel::Error => tracing::error!("{}", msg),
            LogLevel::Warn => tracing::warn!("{}", msg),
            LogLevel::Info => tracing::info!("{}", msg),
            LogLevel::Debug => tracing::debug!("{}", msg),
            LogLevel::Trace => tracing::trace!("{}", msg),
            LogLevel::Off => {}
        }
    }

    fn log_with_fields(&self, level: LogLevel, module: &str, message: &str, fields: &[(&str, &str)]) {
        if let Some(ref config) = self.module_config {
            if !config.allows(module, &level) {
                return;
            }
        }
        let fields_part = fields
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(" ");
        let msg = if fields.is_empty() {
            format!("[{}] {}", module, message)
        } else {
            format!("[{}] {} {}", module, message, fields_part)
        };
        match level {
            LogLevel::Error => tracing::error!("{}", msg),
            LogLevel::Warn => tracing::warn!("{}", msg),
            LogLevel::Info => tracing::info!("{}", msg),
            LogLevel::Debug => tracing::debug!("{}", msg),
            LogLevel::Trace => tracing::trace!("{}", msg),
            LogLevel::Off => {}
        }
    }
}

pub struct ModuleLogConfig {
    default_level: LogLevel,
    overrides: RwLock<HashMap<String, LogLevel>>,
}

impl ModuleLogConfig {
    pub fn new(default_level: LogLevel) -> Self {
        Self {
            default_level,
            overrides: RwLock::new(HashMap::new()),
        }
    }

    pub fn set_level(&self, module: &str, level: LogLevel) {
        let mut map = self.overrides.write().unwrap();
        map.insert(module.to_string(), level);
    }

    pub fn reset(&self, module: &str) {
        let mut map = self.overrides.write().unwrap();
        map.remove(module);
    }

    pub fn reset_all(&self) {
        let mut map = self.overrides.write().unwrap();
        map.clear();
    }

    pub fn level_for(&self, module: &str) -> LogLevel {
        let map = self.overrides.read().unwrap();
        map.get(module).copied().unwrap_or(self.default_level)
    }

    pub fn default_level(&self) -> LogLevel {
        self.default_level
    }

    pub fn allows(&self, module: &str, level: &LogLevel) -> bool {
        self.level_for(module).allows(level)
    }

    pub fn overrides_snapshot(&self) -> HashMap<String, LogLevel> {
        let map = self.overrides.read().unwrap();
        map.clone()
    }
}

impl Default for ModuleLogConfig {
    fn default() -> Self {
        Self::new(LogLevel::Info)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noop_logger() {
        let logger = NoopLogger;
        logger.info("test", "hello");
        logger.error_with("test", "oh no", &[("code", "500")]);
    }

    #[test]
    fn test_module_log_config_default() {
        let config = ModuleLogConfig::new(LogLevel::Info);
        assert!(config.allows("anything", &LogLevel::Info));
        assert!(config.allows("anything", &LogLevel::Warn));
        assert!(!config.allows("anything", &LogLevel::Debug));
    }

    #[test]
    fn test_module_log_config_override() {
        let config = ModuleLogConfig::new(LogLevel::Info);
        config.set_level("verbose_module", LogLevel::Trace);
        assert!(config.allows("verbose_module", &LogLevel::Trace));
        assert!(config.allows("verbose_module", &LogLevel::Debug));
        assert!(config.allows("other_module", &LogLevel::Info));
        assert!(!config.allows("other_module", &LogLevel::Trace));
    }

    #[test]
    fn test_module_log_config_reset() {
        let config = ModuleLogConfig::new(LogLevel::Warn);
        config.set_level("noisy", LogLevel::Trace);
        assert!(config.allows("noisy", &LogLevel::Trace));
        config.reset("noisy");
        assert!(!config.allows("noisy", &LogLevel::Trace));
        assert!(config.allows("noisy", &LogLevel::Warn));
    }

    #[test]
    fn test_tracing_logger_no_filter() {
        let logger = TracingLogger::new();
        logger.info("test", "should appear");
        logger.debug("test", "should appear");
    }

    #[test]
    fn test_tracing_logger_with_filter() {
        let config = Arc::new(ModuleLogConfig::new(LogLevel::Warn));
        let logger = TracingLogger::with_config(config);
        logger.error("mod", "visible");
        logger.warn("mod", "visible");
        logger.info("mod", "filtered out");
    }
}
