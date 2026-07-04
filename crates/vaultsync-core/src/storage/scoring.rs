use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NetworkTier {
    Offline,
    Cellular,
    WiFi,
    Ethernet,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoringContext {
    pub now_ms: u64,
    pub working_set_id: Option<String>,
    pub network_tier: NetworkTier,
    pub memory_pressure: f64,
}

impl Default for ScoringContext {
    fn default() -> Self {
        Self {
            now_ms: 0,
            working_set_id: None,
            network_tier: NetworkTier::WiFi,
            memory_pressure: 0.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactorKind {
    Additive,
    Multiplier,
}

pub trait ScoringFactor: Send + Sync {
    fn name(&self) -> &str;
    fn weight(&self) -> f64;
    fn kind(&self) -> FactorKind;
    fn compute(&self, info: &PageAccessInfo, ctx: &ScoringContext) -> f64;
    fn as_any(&self) -> &dyn std::any::Any;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageAccessInfo {
    pub doc_id: String,
    pub record_id: String,
    pub size_bytes: u64,
    pub last_access_ms: u64,
    pub access_count_last_hour: u32,
    pub pinned: bool,
    pub working_set_id: Option<String>,
}

pub struct CompositeScorer {
    factors: Vec<Box<dyn ScoringFactor>>,
    weights: HashMap<String, f64>,
}

impl std::fmt::Debug for CompositeScorer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompositeScorer")
            .field("factor_count", &self.factors.len())
            .field("weights", &self.weights)
            .finish()
    }
}

impl CompositeScorer {
    pub fn new() -> Self {
        let mut scorer = Self {
            factors: Vec::new(),
            weights: HashMap::new(),
        };
        scorer.add_factor(Box::new(RecencyFactor::new(0.40)));
        scorer.add_factor(Box::new(FrequencyFactor::new(0.30)));
        scorer.add_factor(Box::new(SizePenaltyFactor::new(0.10)));
        scorer.add_factor(Box::new(PredictedAccessFactor::new(0.20)));
        scorer.add_factor(Box::new(PinnedFactor::new(1.0)));
        scorer.add_factor(Box::new(WorkingSetAffinityFactor::new(0.0)));
        scorer.add_factor(Box::new(NetworkAwarenessFactor::new(0.0)));
        scorer
    }

    pub fn add_factor(&mut self, factor: Box<dyn ScoringFactor>) {
        let name = factor.name().to_string();
        let weight = factor.weight();
        self.weights.insert(name, weight);
        self.factors.push(factor);
    }

    pub fn set_weight(&mut self, factor_name: &str, weight: f64) {
        self.weights.insert(factor_name.to_string(), weight);
    }

    pub fn get_weight(&self, factor_name: &str) -> f64 {
        self.weights.get(factor_name).copied().unwrap_or(0.0)
    }

    pub fn compute(&self, info: &PageAccessInfo, ctx: &ScoringContext) -> AccessScore {
        let mut factor_scores = HashMap::new();
        let mut total = 0.0;
        let mut multiplier = 1.0;

        for factor in &self.factors {
            let raw = factor.compute(info, ctx);
            let weight = self.weights.get(factor.name()).copied().unwrap_or(0.0);
            factor_scores.insert(factor.name().to_string(), raw);

            match factor.kind() {
                FactorKind::Additive => {
                    total += raw * weight;
                }
                FactorKind::Multiplier => {
                    multiplier *= raw * weight;
                }
            }
        }

        total *= multiplier;

        AccessScore {
            recency: factor_scores.get("recency").copied().unwrap_or(0.0),
            frequency: factor_scores.get("frequency").copied().unwrap_or(0.0),
            size_penalty: factor_scores.get("size_penalty").copied().unwrap_or(0.0),
            predicted: factor_scores.get("predicted").copied().unwrap_or(0.0),
            working_set_affinity: factor_scores.get("working_set_affinity").copied().unwrap_or(0.0),
            network_awareness: factor_scores.get("network_awareness").copied().unwrap_or(0.0),
            pinned: factor_scores.get("pinned").copied().unwrap_or(1.0),
            total,
        }
    }

    pub fn factor_names(&self) -> Vec<String> {
        self.factors.iter().map(|f| f.name().to_string()).collect()
    }
}

impl Default for CompositeScorer {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessScore {
    pub recency: f64,
    pub frequency: f64,
    pub size_penalty: f64,
    pub predicted: f64,
    pub working_set_affinity: f64,
    pub network_awareness: f64,
    pub pinned: f64,
    pub total: f64,
}

pub struct RecencyFactor {
    weight: f64,
}

impl RecencyFactor {
    pub fn new(weight: f64) -> Self {
        Self { weight }
    }
}

impl ScoringFactor for RecencyFactor {
    fn name(&self) -> &str {
        "recency"
    }

    fn weight(&self) -> f64 {
        self.weight
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Additive
    }

    fn compute(&self, info: &PageAccessInfo, ctx: &ScoringContext) -> f64 {
        let elapsed_secs = ((ctx.now_ms.saturating_sub(info.last_access_ms)) as f64 / 1000.0).max(1.0);
        (1.0 / elapsed_secs).min(1.0)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct FrequencyFactor {
    weight: f64,
}

impl FrequencyFactor {
    pub fn new(weight: f64) -> Self {
        Self { weight }
    }
}

impl ScoringFactor for FrequencyFactor {
    fn name(&self) -> &str {
        "frequency"
    }

    fn weight(&self) -> f64 {
        self.weight
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Additive
    }

    fn compute(&self, info: &PageAccessInfo, _ctx: &ScoringContext) -> f64 {
        (info.access_count_last_hour as f64 / 3600.0).min(1.0)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct SizePenaltyFactor {
    weight: f64,
}

impl SizePenaltyFactor {
    pub fn new(weight: f64) -> Self {
        Self { weight }
    }
}

impl ScoringFactor for SizePenaltyFactor {
    fn name(&self) -> &str {
        "size_penalty"
    }

    fn weight(&self) -> f64 {
        self.weight
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Additive
    }

    fn compute(&self, info: &PageAccessInfo, _ctx: &ScoringContext) -> f64 {
        1.0 / (info.size_bytes.max(1) as f64).log2().max(1.0)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct PredictedAccessFactor {
    weight: f64,
}

impl PredictedAccessFactor {
    pub fn new(weight: f64) -> Self {
        Self { weight }
    }
}

impl ScoringFactor for PredictedAccessFactor {
    fn name(&self) -> &str {
        "predicted"
    }

    fn weight(&self) -> f64 {
        self.weight
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Additive
    }

    fn compute(&self, _info: &PageAccessInfo, _ctx: &ScoringContext) -> f64 {
        0.0
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct PinnedFactor {
    weight: f64,
}

impl PinnedFactor {
    pub fn new(weight: f64) -> Self {
        Self { weight }
    }
}

impl ScoringFactor for PinnedFactor {
    fn name(&self) -> &str {
        "pinned"
    }

    fn weight(&self) -> f64 {
        self.weight
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Multiplier
    }

    fn compute(&self, info: &PageAccessInfo, _ctx: &ScoringContext) -> f64 {
        if info.pinned {
            100.0
        } else {
            1.0
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct WorkingSetAffinityFactor {
    weight: f64,
}

impl WorkingSetAffinityFactor {
    pub fn new(weight: f64) -> Self {
        Self { weight }
    }
}

impl ScoringFactor for WorkingSetAffinityFactor {
    fn name(&self) -> &str {
        "working_set_affinity"
    }

    fn weight(&self) -> f64 {
        self.weight
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Additive
    }

    fn compute(&self, info: &PageAccessInfo, ctx: &ScoringContext) -> f64 {
        match (&info.working_set_id, &ctx.working_set_id) {
            (Some(doc_ws), Some(active_ws)) if doc_ws == active_ws => 1.0,
            _ => 0.0,
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct NetworkAwarenessFactor {
    weight: f64,
}

impl NetworkAwarenessFactor {
    pub fn new(weight: f64) -> Self {
        Self { weight }
    }
}

impl ScoringFactor for NetworkAwarenessFactor {
    fn name(&self) -> &str {
        "network_awareness"
    }

    fn weight(&self) -> f64 {
        self.weight
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Additive
    }

    fn compute(&self, info: &PageAccessInfo, ctx: &ScoringContext) -> f64 {
        let size_mb = info.size_bytes as f64 / (1024.0 * 1024.0);
        let tier_penalty = match ctx.network_tier {
            NetworkTier::Offline => 1.0,
            NetworkTier::Cellular => 0.8,
            NetworkTier::WiFi => 0.3,
            NetworkTier::Ethernet => 0.0,
        };
        (size_mb * tier_penalty).min(1.0)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_info() -> PageAccessInfo {
        PageAccessInfo {
            doc_id: "d1".into(),
            record_id: "r1".into(),
            size_bytes: 4096,
            last_access_ms: 1000,
            access_count_last_hour: 60,
            pinned: false,
            working_set_id: None,
        }
    }

    fn test_ctx() -> ScoringContext {
        ScoringContext {
            now_ms: 2000,
            working_set_id: None,
            network_tier: NetworkTier::WiFi,
            memory_pressure: 0.0,
        }
    }

    #[test]
    fn test_composite_scorer_default_factors() {
        let scorer = CompositeScorer::new();
        let names = scorer.factor_names();
        assert!(names.contains(&"recency".to_string()));
        assert!(names.contains(&"frequency".to_string()));
        assert!(names.contains(&"size_penalty".to_string()));
        assert!(names.contains(&"predicted".to_string()));
        assert!(names.contains(&"pinned".to_string()));
        assert!(names.contains(&"working_set_affinity".to_string()));
        assert!(names.contains(&"network_awareness".to_string()));
    }

    #[test]
    fn test_recency_factor() {
        let factor = RecencyFactor::new(1.0);
        let info = test_info();
        let ctx = test_ctx();
        let score = factor.compute(&info, &ctx);
        assert!(score > 0.0 && score <= 1.0);
    }

    #[test]
    fn test_frequency_factor() {
        let factor = FrequencyFactor::new(1.0);
        let info = test_info();
        let ctx = test_ctx();
        let score = factor.compute(&info, &ctx);
        assert!(score > 0.0 && score <= 1.0);
    }

    #[test]
    fn test_size_penalty_factor() {
        let factor = SizePenaltyFactor::new(1.0);
        let info = test_info();
        let ctx = test_ctx();
        let score = factor.compute(&info, &ctx);
        assert!(score > 0.0 && score <= 1.0);
    }

    #[test]
    fn test_pinned_factor() {
        let factor = PinnedFactor::new(1.0);
        let mut info = test_info();
        let ctx = test_ctx();

        info.pinned = false;
        assert_eq!(factor.compute(&info, &ctx), 1.0);

        info.pinned = true;
        assert_eq!(factor.compute(&info, &ctx), 100.0);
    }

    #[test]
    fn test_pinned_factor_is_multiplier() {
        let factor = PinnedFactor::new(1.0);
        assert_eq!(factor.kind(), FactorKind::Multiplier);
    }

    #[test]
    fn test_working_set_affinity_factor() {
        let factor = WorkingSetAffinityFactor::new(1.0);
        let mut info = test_info();
        let mut ctx = test_ctx();

        info.working_set_id = Some("ws1".into());
        ctx.working_set_id = Some("ws1".into());
        assert_eq!(factor.compute(&info, &ctx), 1.0);

        ctx.working_set_id = Some("ws2".into());
        assert_eq!(factor.compute(&info, &ctx), 0.0);
    }

    #[test]
    fn test_network_awareness_factor() {
        let factor = NetworkAwarenessFactor::new(1.0);
        let info = test_info();
        let mut ctx = test_ctx();

        ctx.network_tier = NetworkTier::Ethernet;
        assert_eq!(factor.compute(&info, &ctx), 0.0);

        ctx.network_tier = NetworkTier::Offline;
        let score = factor.compute(&info, &ctx);
        assert!(score > 0.0);
    }

    #[test]
    fn test_composite_scorer_computes_total() {
        let scorer = CompositeScorer::new();
        let info = test_info();
        let ctx = test_ctx();
        let score = scorer.compute(&info, &ctx);
        assert!(score.total > 0.0);
    }

    #[test]
    fn test_composite_scorer_weight_override() {
        let mut scorer = CompositeScorer::new();
        scorer.set_weight("recency", 0.80);
        scorer.set_weight("frequency", 0.10);
        scorer.set_weight("size_penalty", 0.05);
        scorer.set_weight("predicted", 0.05);

        let info = test_info();
        let ctx = test_ctx();
        let score = scorer.compute(&info, &ctx);
        assert!(score.total > 0.0);
        assert_eq!(scorer.get_weight("recency"), 0.80);
    }

    #[test]
    fn test_predicted_factor_returns_zero() {
        let factor = PredictedAccessFactor::new(1.0);
        let info = test_info();
        let ctx = test_ctx();
        assert_eq!(factor.compute(&info, &ctx), 0.0);
    }

    #[test]
    fn test_pinned_multiplier_dominates_score() {
        let scorer = CompositeScorer::new();
        let mut info = test_info();
        info.last_access_ms = 0;
        info.access_count_last_hour = 0;
        let ctx = ScoringContext {
            now_ms: 1_000_000_000,
            working_set_id: None,
            network_tier: NetworkTier::WiFi,
            memory_pressure: 0.0,
        };

        info.pinned = false;
        let unpinned_score = scorer.compute(&info, &ctx);

        info.pinned = true;
        let pinned_score = scorer.compute(&info, &ctx);

        assert!(pinned_score.total > unpinned_score.total * 10.0);
    }
}
