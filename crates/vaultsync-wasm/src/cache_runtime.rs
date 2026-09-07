use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use async_trait::async_trait;

use crate::metrics::RuntimeMetrics;
use vaultsync_core::runtime_state::RuntimeLifecycle;

/// CacheCategory — identifies which cache to operate on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheCategory {
    /// Page data cache (per-PageStore LRU)
    Page,
    /// Document cache (SegmentedLruCache)
    Document,
    /// Mirror cache (follower document snapshots)
    Mirror,
    /// Query result cache
    Query,
    /// Manifest cache (parsed StoreManifest entries)
    Manifest,
}

impl CacheCategory {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Page => "page",
            Self::Document => "document",
            Self::Mirror => "mirror",
            Self::Query => "query",
            Self::Manifest => "manifest",
        }
    }
}

/// CacheRuntime — centralized eviction coordination.
///
/// Owns no cache storage directly. Instead, CacheConsumer implementations
/// register with CacheRuntime. When eviction is needed, CacheRuntime
/// notifies consumers in priority order.
///
/// This avoids coupling CacheRuntime to any specific cache implementation
/// while still providing coordinated eviction across the engine.
pub struct CacheRuntime {
    metrics: Arc<RuntimeMetrics>,

    /// Total memory budget across all caches (bytes)
    memory_budget: AtomicU64,

    /// Current estimated usage across all caches
    estimated_usage: AtomicU64,

    /// Eviction watermark (fraction of budget). Eviction triggers when usage exceeds this.
    eviction_watermark: AtomicU64, // stored as parts-per-thousand (0-1000)
}

impl fmt::Debug for CacheRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CacheRuntime")
            .field("memory_budget", &self.memory_budget)
            .field("estimated_usage", &self.estimated_usage)
            .field("eviction_watermark", &self.eviction_watermark)
            .finish()
    }
}

impl CacheRuntime {
    /// Default budget: 256 MB
    const DEFAULT_BUDGET: u64 = 256 * 1024 * 1024;
    /// Eviction watermark: 80% (800/1000)
    const DEFAULT_WATERMARK: u64 = 800;

    pub fn new(metrics: Arc<RuntimeMetrics>) -> Self {
        Self {
            metrics,
            memory_budget: AtomicU64::new(Self::DEFAULT_BUDGET),
            estimated_usage: AtomicU64::new(0),
            eviction_watermark: AtomicU64::new(Self::DEFAULT_WATERMARK),
        }
    }

    pub fn set_budget(&self, bytes: u64) {
        self.memory_budget.store(bytes, Ordering::Release);
    }

    pub fn budget(&self) -> u64 {
        self.memory_budget.load(Ordering::Acquire)
    }

    pub fn set_watermark(&self, ppt: u64) {
        self.eviction_watermark.store(ppt.min(1000), Ordering::Release);
    }

    pub fn watermark(&self) -> u64 {
        self.eviction_watermark.load(Ordering::Acquire)
    }

    /// Report cache usage for a category (called by consumers).
    pub fn report_usage(&self, _category: CacheCategory, bytes: u64) {
        self.estimated_usage.store(bytes, Ordering::Release);
    }

    pub fn estimated_usage(&self) -> u64 {
        self.estimated_usage.load(Ordering::Acquire)
    }

    /// Check if eviction is needed based on watermark.
    pub fn should_evict(&self) -> bool {
        let budget = self.budget();
        if budget == 0 {
            return false;
        }
        let usage = self.estimated_usage();
        let limit = budget * self.watermark() / 1000;
        usage > limit
    }

    /// Calculate how many bytes to evict to get below the watermark.
    pub fn eviction_target(&self) -> u64 {
        if !self.should_evict() {
            return 0;
        }
        let budget = self.budget();
        let usage = self.estimated_usage();
        let target = budget * self.watermark() / 1000;
        usage.saturating_sub(target)
    }

    pub fn record_eviction(&self, _category: CacheCategory, bytes: u64) {
        let current = self.estimated_usage.load(Ordering::Acquire);
        self.estimated_usage
            .store(current.saturating_sub(bytes), Ordering::Release);
    }

    /// Clear all cache usage tracking.
    pub fn clear(&self) {
        self.estimated_usage.store(0, Ordering::Release);
    }
}

#[async_trait]
impl RuntimeLifecycle for CacheRuntime {
    async fn boot(&self) -> Result<(), String> { Ok(()) }
    async fn ready(&self) -> Result<(), String> {
        engine_debug!("[cache] ready: budget={}MB watermark={}%",
            self.budget() / (1024 * 1024),
            self.watermark() / 10);
        Ok(())
    }
    async fn warm(&self) -> Result<(), String> { Ok(()) }
    async fn idle(&self) -> Result<(), String> { Ok(()) }
    async fn shutdown(&self) -> Result<(), String> {
        self.clear();
        Ok(())
    }
}
