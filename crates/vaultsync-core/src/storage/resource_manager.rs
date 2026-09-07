use crate::storage::scoring::{AccessScore, CompositeScorer, NetworkTier, PageAccessInfo as ScoringPageAccessInfo, ScoringContext};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageContext {
    pub workspace_id: Option<u64>,
    pub working_set_id: Option<String>,
    pub priority: PagePriority,
    pub pinned: bool,
}

impl Default for PageContext {
    fn default() -> Self {
        Self {
            workspace_id: None,
            working_set_id: None,
            priority: PagePriority::Normal,
            pinned: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PagePriority {
    Low,
    Normal,
    High,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MemoryTier {
    ResidentActive,
    ResidentHot,
    RecoverableWarm,
    RecoverableCold,
    Archived,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryBudget {
    pub active_bytes: u64,
    pub hot_bytes: u64,
    pub warm_bytes: u64,
    pub snapshot_cache: u64,
    pub indexes: u64,
    pub scratch: u64,
}

impl Default for MemoryBudget {
    fn default() -> Self {
        Self {
            active_bytes: 150 * 1024 * 1024,
            hot_bytes: 100 * 1024 * 1024,
            warm_bytes: 150 * 1024 * 1024,
            snapshot_cache: 50 * 1024 * 1024,
            indexes: 30 * 1024 * 1024,
            scratch: 32 * 1024 * 1024,
        }
    }
}

impl MemoryBudget {
    pub fn total(&self) -> u64 {
        self.active_bytes
            + self.hot_bytes
            + self.warm_bytes
            + self.snapshot_cache
            + self.indexes
            + self.scratch
    }

    pub fn budget_for_tier(&self, tier: MemoryTier) -> u64 {
        match tier {
            MemoryTier::ResidentActive => self.active_bytes,
            MemoryTier::ResidentHot => self.hot_bytes,
            MemoryTier::RecoverableWarm => self.warm_bytes,
            MemoryTier::RecoverableCold => self.warm_bytes,
            MemoryTier::Archived => 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageAccessInfo {
    pub doc_id: String,
    pub record_id: String,
    pub size_bytes: u64,
    pub last_access_ms: u64,
    pub access_count_last_hour: u32,
    pub tier: MemoryTier,
    pub pinned: bool,
    pub working_set_id: Option<String>,
}

impl PageAccessInfo {
    pub fn to_scoring_info(&self) -> ScoringPageAccessInfo {
        ScoringPageAccessInfo {
            doc_id: self.doc_id.clone(),
            record_id: self.record_id.clone(),
            size_bytes: self.size_bytes,
            last_access_ms: self.last_access_ms,
            access_count_last_hour: self.access_count_last_hour,
            pinned: self.pinned,
            working_set_id: self.working_set_id.clone(),
        }
    }

    pub fn tier_budget_key(&self) -> MemoryTier {
        if self.pinned {
            MemoryTier::ResidentActive
        } else {
            self.tier
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceManagerStats {
    pub pages_tracked: u64,
    pub total_bytes: u64,
    pub budget_bytes: u64,
    pub active_bytes: u64,
    pub hot_bytes: u64,
    pub warm_bytes: u64,
    pub archived_bytes: u64,
    pub eviction_candidates: u64,
    pub promotions_last_sweep: u64,
    pub demotions_last_sweep: u64,
}

#[derive(Debug)]
pub struct ResourceManager {
    budget: MemoryBudget,
    pages: RwLock<HashMap<(String, String), PageAccessInfo>>,
    stats: RwLock<ResourceManagerStats>,
    scorer: RwLock<CompositeScorer>,
    network_tier: RwLock<NetworkTier>,
}

impl ResourceManager {
    pub fn new(budget: MemoryBudget) -> Self {
        Self {
            budget,
            pages: RwLock::new(HashMap::new()),
            stats: RwLock::new(ResourceManagerStats::default()),
            scorer: RwLock::new(CompositeScorer::new()),
            network_tier: RwLock::new(NetworkTier::WiFi),
        }
    }

    pub fn with_scorer(budget: MemoryBudget, scorer: CompositeScorer) -> Self {
        Self {
            budget,
            pages: RwLock::new(HashMap::new()),
            stats: RwLock::new(ResourceManagerStats::default()),
            scorer: RwLock::new(scorer),
            network_tier: RwLock::new(NetworkTier::WiFi),
        }
    }

    pub fn set_network_tier(&self, tier: NetworkTier) {
        if let Ok(mut nt) = self.network_tier.write() {
            *nt = tier;
        }
    }

    pub fn network_tier(&self) -> NetworkTier {
        self.network_tier.read().map(|g| *g).unwrap_or(NetworkTier::WiFi)
    }

    pub fn budget(&self) -> &MemoryBudget {
        &self.budget
    }

    pub fn scorer(&self) -> &RwLock<CompositeScorer> {
        &self.scorer
    }

    pub fn set_scoring_weight(&self, factor_name: &str, weight: f64) {
        if let Ok(mut scorer) = self.scorer.write() {
            scorer.set_weight(factor_name, weight);
        }
    }

    fn compute_score(&self, info: &PageAccessInfo, now_ms: u64) -> AccessScore {
        let network_tier = self.network_tier.read().map(|g| *g).unwrap_or(NetworkTier::WiFi);
        let ctx = ScoringContext {
            now_ms,
            working_set_id: info.working_set_id.clone(),
            network_tier,
            memory_pressure: 0.0,
        };
        let scoring_info = info.to_scoring_info();
        if let Ok(scorer) = self.scorer.read() {
            scorer.compute(&scoring_info, &ctx)
        } else {
            AccessScore {
                recency: 0.0,
                frequency: 0.0,
                size_penalty: 0.0,
                predicted: 0.0,
                working_set_affinity: 0.0,
                network_awareness: 0.0,
                pinned: 1.0,
                total: 0.0,
            }
        }
    }

    pub fn record_access(&self, doc_id: &str, record_id: &str, size_bytes: u64, now_ms: u64) {
        self.record_access_with_context(doc_id, record_id, size_bytes, now_ms, PageContext::default());
    }

    pub fn record_access_with_context(
        &self,
        doc_id: &str,
        record_id: &str,
        size_bytes: u64,
        now_ms: u64,
        ctx: PageContext,
    ) {
        if let Ok(mut pages) = self.pages.write() {
            let key = (doc_id.to_string(), record_id.to_string());
            if let Some(info) = pages.get_mut(&key) {
                if now_ms.saturating_sub(info.last_access_ms) < 3600_000 {
                    info.access_count_last_hour = info.access_count_last_hour.saturating_add(1);
                } else {
                    info.access_count_last_hour = 1;
                }
                info.last_access_ms = now_ms;
                if ctx.pinned {
                    info.pinned = true;
                    info.tier = MemoryTier::ResidentActive;
                }
                if let Some(ref ws_id) = ctx.working_set_id {
                    info.working_set_id = Some(ws_id.clone());
                }
                if info.tier == MemoryTier::RecoverableCold || info.tier == MemoryTier::Archived {
                    info.tier = MemoryTier::ResidentHot;
                }
            } else {
                pages.insert(
                    key,
                    PageAccessInfo {
                        doc_id: doc_id.to_string(),
                        record_id: record_id.to_string(),
                        size_bytes,
                        last_access_ms: now_ms,
                        access_count_last_hour: 1,
                        tier: if ctx.pinned { MemoryTier::ResidentActive } else { MemoryTier::ResidentHot },
                        pinned: ctx.pinned,
                        working_set_id: ctx.working_set_id,
                    },
                );
            }
        }
    }

    pub fn remove_page(&self, doc_id: &str, record_id: &str) {
        if let Ok(mut pages) = self.pages.write() {
            pages.remove(&(doc_id.to_string(), record_id.to_string()));
        }
    }

    pub fn set_pinned(&self, doc_id: &str, record_id: &str, pinned: bool) {
        if let Ok(mut pages) = self.pages.write() {
            let key = (doc_id.to_string(), record_id.to_string());
            if let Some(info) = pages.get_mut(&key) {
                info.pinned = pinned;
                if pinned {
                    info.tier = MemoryTier::ResidentActive;
                }
            }
        }
    }

    pub fn set_working_set(&self, doc_id: &str, record_id: &str, working_set_id: Option<String>) {
        if let Ok(mut pages) = self.pages.write() {
            let key = (doc_id.to_string(), record_id.to_string());
            if let Some(info) = pages.get_mut(&key) {
                info.working_set_id = working_set_id;
            }
        }
    }

    pub fn get_tier(&self, doc_id: &str, record_id: &str) -> Option<MemoryTier> {
        self.pages
            .read()
            .ok()?
            .get(&(doc_id.to_string(), record_id.to_string()))
            .map(|info| info.tier)
    }

    pub fn eviction_candidates(
        &self,
        tier: MemoryTier,
        target_bytes: u64,
        now_ms: u64,
    ) -> Vec<(String, String, f64)> {
        let pages = match self.pages.read() {
            Ok(p) => p,
            Err(_) => return Vec::new(),
        };

        let mut candidates: Vec<(String, String, f64)> = pages
            .iter()
            .filter(|(_, info)| info.tier_budget_key() == tier && !info.pinned)
              .map(|((doc_id, record_id), info)| {
                let score = self.compute_score(info, now_ms);
                (doc_id.clone(), record_id.clone(), score.total)
            })
            .collect();

        candidates.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal));

        let mut selected = Vec::new();
        let mut freed: u64 = 0;
        for (doc_id, record_id, score) in &candidates {
            if freed >= target_bytes {
                break;
            }
            if let Some(info) = pages.get(&(doc_id.clone(), record_id.clone())) {
                freed += info.size_bytes;
                selected.push((doc_id.clone(), record_id.clone(), *score));
            }
        }
        selected
    }

    pub fn promote_to_resident(&self, doc_id: &str, record_id: &str) {
        if let Ok(mut pages) = self.pages.write() {
            let key = (doc_id.to_string(), record_id.to_string());
            if let Some(info) = pages.get_mut(&key) {
                info.tier = MemoryTier::ResidentHot;
            }
        }
    }

    pub fn demote_to_recoverable(&self, doc_id: &str, record_id: &str) {
        if let Ok(mut pages) = self.pages.write() {
            let key = (doc_id.to_string(), record_id.to_string());
            if let Some(info) = pages.get_mut(&key) {
                if info.tier == MemoryTier::ResidentHot {
                    info.tier = MemoryTier::RecoverableWarm;
                }
            }
        }
    }

    pub fn sweep(&self, now_ms: u64) -> ResourceManagerStats {
        let mut pages = match self.pages.write() {
            Ok(p) => p,
            Err(_) => return ResourceManagerStats::default(),
        };

        let scores: Vec<(String, String, f64, u64, MemoryTier, bool)> = pages
            .iter()
            .map(|((doc_id, record_id), info)| {
                let score = self.compute_score(info, now_ms);
                (
                    doc_id.clone(),
                    record_id.clone(),
                    score.total,
                    info.size_bytes,
                    info.tier,
                    info.pinned,
                )
            })
            .collect();

        fn tier_rank(tier: &MemoryTier) -> u8 {
            match tier {
                MemoryTier::ResidentActive => 0,
                MemoryTier::ResidentHot => 1,
                MemoryTier::RecoverableWarm => 2,
                MemoryTier::RecoverableCold => 3,
                MemoryTier::Archived => 4,
            }
        }

        let mut promotions = 0u64;
        let mut demotions = 0u64;
        let mut active_bytes = 0u64;
        let mut hot_bytes = 0u64;
        let mut warm_bytes = 0u64;
        let mut archived_bytes = 0u64;

        for (doc_id, record_id, score, size_bytes, tier, pinned) in &scores {
            match tier {
                MemoryTier::ResidentActive => active_bytes += *size_bytes,
                MemoryTier::ResidentHot => hot_bytes += *size_bytes,
                MemoryTier::RecoverableWarm | MemoryTier::RecoverableCold => {
                    warm_bytes += *size_bytes
                }
                MemoryTier::Archived => archived_bytes += *size_bytes,
            }

            if *pinned {
                continue;
            }

            let target_tier = tier_from_score(*score);
            let current_rank = tier_rank(tier);
            let target_rank = tier_rank(&target_tier);
            if target_rank > current_rank {
                if let Some(info) = pages.get_mut(&(doc_id.clone(), record_id.clone())) {
                    info.tier = target_tier;
                    demotions += 1;
                }
            } else if target_rank < current_rank {
                if let Some(info) = pages.get_mut(&(doc_id.clone(), record_id.clone())) {
                    info.tier = target_tier;
                    promotions += 1;
                }
            }
        }

        let total_bytes = active_bytes + hot_bytes + warm_bytes + archived_bytes;
        let hot_budget = self.budget.hot_bytes;
        let warm_budget = self.budget.warm_bytes;

        let eviction_candidates = if hot_bytes > hot_budget {
            hot_bytes - hot_budget
        } else if warm_bytes > warm_budget {
            warm_bytes - warm_budget
        } else {
            0
        };

        let stats = ResourceManagerStats {
            pages_tracked: pages.len() as u64,
            total_bytes,
            budget_bytes: self.budget.total(),
            active_bytes,
            hot_bytes,
            warm_bytes,
            archived_bytes,
            eviction_candidates,
            promotions_last_sweep: promotions,
            demotions_last_sweep: demotions,
        };

        if let Ok(mut s) = self.stats.write() {
            *s = stats.clone();
        }

        stats
    }

    pub fn stats(&self) -> ResourceManagerStats {
        self.stats
            .read()
            .map(|g| g.clone())
            .unwrap_or_default()
    }

    pub fn clear(&self) {
        if let Ok(mut pages) = self.pages.write() {
            pages.clear();
        }
    }
}

fn tier_from_score(score: f64) -> MemoryTier {
    if score >= 10.0 {
        MemoryTier::ResidentActive
    } else if score >= 1.0 {
        MemoryTier::ResidentHot
    } else if score >= 0.3 {
        MemoryTier::RecoverableWarm
    } else if score >= 0.1 {
        MemoryTier::RecoverableCold
    } else {
        MemoryTier::Archived
    }
}

impl Default for ResourceManager {
    fn default() -> Self {
        Self::new(MemoryBudget::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_budget_total() {
        let budget = MemoryBudget::default();
        assert_eq!(budget.total(), 512 * 1024 * 1024);
    }

    #[test]
    fn test_access_score_pinned() {
        let rm = ResourceManager::default();
        let info = PageAccessInfo {
            doc_id: "d1".into(),
            record_id: "r1".into(),
            size_bytes: 4096,
            last_access_ms: 1000,
            access_count_last_hour: 60,
            tier: MemoryTier::ResidentHot,
            pinned: true,
            working_set_id: None,
        };
        let score = rm.compute_score(&info, 2000);
        assert!(score.total > 30.0);
        assert_eq!(score.pinned, 100.0);
    }

    #[test]
    fn test_access_score_unpinned() {
        let rm = ResourceManager::default();
        let info = PageAccessInfo {
            doc_id: "d1".into(),
            record_id: "r1".into(),
            size_bytes: 4096,
            last_access_ms: 1000,
            access_count_last_hour: 60,
            tier: MemoryTier::ResidentHot,
            pinned: false,
            working_set_id: None,
        };
        let score = rm.compute_score(&info, 2000);
        assert!(score.total < 2.0);
        assert_eq!(score.pinned, 1.0);
    }

    #[test]
    fn test_record_access_new_page() {
        let rm = ResourceManager::default();
        rm.record_access("d1", "r1", 4096, 1000);
        let tier = rm.get_tier("d1", "r1");
        assert_eq!(tier, Some(MemoryTier::ResidentHot));
    }

    #[test]
    fn test_record_access_promotes_from_cold() {
        let rm = ResourceManager::default();
        {
            let mut pages = rm.pages.write().unwrap();
            pages.insert(
                ("d1".into(), "r1".into()),
                PageAccessInfo {
                    doc_id: "d1".into(),
                    record_id: "r1".into(),
                    size_bytes: 4096,
                    last_access_ms: 100,
                    access_count_last_hour: 0,
                    tier: MemoryTier::RecoverableCold,
                    pinned: false,
                    working_set_id: None,
                },
            );
        }
        rm.record_access("d1", "r1", 4096, 2000);
        let tier = rm.get_tier("d1", "r1");
        assert_eq!(tier, Some(MemoryTier::ResidentHot));
    }

    #[test]
    fn test_eviction_candidates() {
        let rm = ResourceManager::default();
        rm.record_access("hot1", "r1", 1000, 1000);
        rm.record_access("hot2", "r2", 2000, 2000);
        rm.record_access("cold1", "r3", 500, 100_000);
        rm.set_pinned("hot1", "r1", false);

        {
            let mut pages = rm.pages.write().unwrap();
            if let Some(info) = pages.get_mut(&("cold1".into(), "r3".into())) {
                info.last_access_ms = 100_000;
                info.tier = MemoryTier::RecoverableWarm;
            }
            if let Some(info) = pages.get_mut(&("hot1".into(), "r1".into())) {
                info.last_access_ms = 1000;
            }
            if let Some(info) = pages.get_mut(&("hot2".into(), "r2".into())) {
                info.last_access_ms = 2000;
            }
        }

        let candidates = rm.eviction_candidates(MemoryTier::ResidentHot, 5000, 200_000);
        assert!(!candidates.is_empty());
    }

    #[test]
    fn test_sweep_demotes_cold_page() {
        let rm = ResourceManager::default();
        rm.record_access("d1", "r1", 4096, 1000);
        {
            let mut pages = rm.pages.write().unwrap();
            if let Some(info) = pages.get_mut(&("d1".into(), "r1".into())) {
                info.last_access_ms = 1000;
                info.access_count_last_hour = 0;
            }
        }
        let result = rm.sweep(1_000_000_000);
        assert!(result.demotions_last_sweep >= 1);
        let tier = rm.get_tier("d1", "r1");
        assert!(tier != Some(MemoryTier::ResidentHot));
    }

    #[test]
    fn test_pinned_stays_active() {
        let rm = ResourceManager::default();
        rm.record_access("d1", "r1", 4096, 1000);
        rm.set_pinned("d1", "r1", true);
        let tier = rm.get_tier("d1", "r1");
        assert_eq!(tier, Some(MemoryTier::ResidentActive));

        let _stats = rm.sweep(1_000_000_000);
        let tier = rm.get_tier("d1", "r1");
        assert_eq!(tier, Some(MemoryTier::ResidentActive));
    }

    #[test]
    fn test_tier_from_score_thresholds() {
        assert_eq!(tier_from_score(50.0), MemoryTier::ResidentActive);
        assert_eq!(tier_from_score(10.0), MemoryTier::ResidentActive);
        assert_eq!(tier_from_score(5.0), MemoryTier::ResidentHot);
        assert_eq!(tier_from_score(1.0), MemoryTier::ResidentHot);
        assert_eq!(tier_from_score(0.5), MemoryTier::RecoverableWarm);
        assert_eq!(tier_from_score(0.3), MemoryTier::RecoverableWarm);
        assert_eq!(tier_from_score(0.2), MemoryTier::RecoverableCold);
        assert_eq!(tier_from_score(0.1), MemoryTier::RecoverableCold);
        assert_eq!(tier_from_score(0.05), MemoryTier::Archived);
    }

    #[test]
    fn test_set_scoring_weight() {
        let rm = ResourceManager::default();
        rm.set_scoring_weight("recency", 0.80);
        rm.set_scoring_weight("frequency", 0.10);
        rm.set_scoring_weight("size_penalty", 0.05);
        rm.set_scoring_weight("predicted", 0.05);

        let info = PageAccessInfo {
            doc_id: "d1".into(),
            record_id: "r1".into(),
            size_bytes: 4096,
            last_access_ms: 1000,
            access_count_last_hour: 60,
            tier: MemoryTier::ResidentHot,
            pinned: false,
            working_set_id: None,
        };
        let score = rm.compute_score(&info, 2000);
        assert!(score.total > 0.0);
    }

    #[test]
    fn test_working_set_affinity() {
        let rm = ResourceManager::default();
        rm.set_scoring_weight("working_set_affinity", 1.0);

        let info = PageAccessInfo {
            doc_id: "d1".into(),
            record_id: "r1".into(),
            size_bytes: 4096,
            last_access_ms: 1000,
            access_count_last_hour: 60,
            tier: MemoryTier::ResidentHot,
            pinned: false,
            working_set_id: Some("ws1".into()),
        };

        let ctx_no_ws = ScoringContext {
            now_ms: 2000,
            working_set_id: None,
            network_tier: NetworkTier::WiFi,
            memory_pressure: 0.0,
        };
        let scoring_info = info.to_scoring_info();
        let score_no_ws = rm.scorer.read().unwrap().compute(&scoring_info, &ctx_no_ws);
        assert_eq!(score_no_ws.working_set_affinity, 0.0);

        let ctx_ws1 = ScoringContext {
            now_ms: 2000,
            working_set_id: Some("ws1".into()),
            network_tier: NetworkTier::WiFi,
            memory_pressure: 0.0,
        };
        let score_ws1 = rm.scorer.read().unwrap().compute(&scoring_info, &ctx_ws1);
        assert_eq!(score_ws1.working_set_affinity, 1.0);
    }
}
