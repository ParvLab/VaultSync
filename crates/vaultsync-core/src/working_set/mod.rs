use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;

pub mod query;

pub use query::QueryFilter;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkingSetId(pub u64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkingSet {
    pub id: WorkingSetId,
    pub name: String,
    pub workspace_id: Option<u64>,
    pub filter: QueryFilter,
    pub policy: WorkingSetPolicy,
    pub created_at: u64,
    pub updated_at: u64,
    pub last_accessed_at: u64,
    pub access_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkingSetPolicy {
    Manual,
    Automatic(AutoPolicy),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoPolicy {
    pub max_documents: usize,
    pub max_age_ms: u64,
    pub eviction_strategy: EvictionStrategy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EvictionStrategy {
    LeastRecentlyUsed,
    LeastFrequentlyUsed,
    FirstInFirstOut,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkingSetInfo {
    pub id: WorkingSetId,
    pub name: String,
    pub workspace_id: Option<u64>,
    pub document_count: usize,
    pub policy: WorkingSetPolicy,
    pub created_at: u64,
    pub last_accessed_at: u64,
}

impl From<&WorkingSet> for WorkingSetInfo {
    fn from(ws: &WorkingSet) -> Self {
        Self {
            id: ws.id,
            name: ws.name.clone(),
            workspace_id: ws.workspace_id,
            document_count: 0,
            policy: ws.policy.clone(),
            created_at: ws.created_at,
            last_accessed_at: ws.last_accessed_at,
        }
    }
}

pub struct WorkingSetManager {
    sets: RwLock<HashMap<WorkingSetId, WorkingSet>>,
    active_set: RwLock<Option<WorkingSetId>>,
    next_id: RwLock<u64>,
}

impl WorkingSetManager {
    pub fn new() -> Self {
        Self {
            sets: RwLock::new(HashMap::new()),
            active_set: RwLock::new(None),
            next_id: RwLock::new(1),
        }
    }

    pub fn create_set(
        &self,
        name: &str,
        filter: QueryFilter,
        policy: WorkingSetPolicy,
        workspace_id: Option<u64>,
        now_ms: u64,
    ) -> WorkingSetId {
        let id = {
            let mut next = self.next_id.write().unwrap();
            let id = WorkingSetId(*next);
            *next += 1;
            id
        };

        let working_set = WorkingSet {
            id,
            name: name.to_string(),
            workspace_id,
            filter,
            policy,
            created_at: now_ms,
            updated_at: now_ms,
            last_accessed_at: now_ms,
            access_count: 0,
        };

        if let Ok(mut sets) = self.sets.write() {
            sets.insert(id, working_set);
        }

        id
    }

    pub fn get_set(&self, id: &WorkingSetId) -> Option<WorkingSet> {
        self.sets.read().ok()?.get(id).cloned()
    }

    pub fn list_sets(&self) -> Vec<WorkingSetInfo> {
        match self.sets.read() {
            Ok(sets) => sets.values().map(WorkingSetInfo::from).collect(),
            Err(_) => Vec::new(),
        }
    }

    pub fn update_set(
        &self,
        id: &WorkingSetId,
        name: Option<&str>,
        filter: Option<QueryFilter>,
        policy: Option<WorkingSetPolicy>,
        now_ms: u64,
    ) -> bool {
        if let Ok(mut sets) = self.sets.write() {
            if let Some(ws) = sets.get_mut(id) {
                if let Some(n) = name {
                    ws.name = n.to_string();
                }
                if let Some(f) = filter {
                    ws.filter = f;
                }
                if let Some(p) = policy {
                    ws.policy = p;
                }
                ws.updated_at = now_ms;
                return true;
            }
        }
        false
    }

    pub fn delete_set(&self, id: &WorkingSetId) -> bool {
        if let Ok(mut sets) = self.sets.write() {
            if sets.remove(id).is_some() {
                if let Ok(mut active) = self.active_set.write() {
                    if *active == Some(*id) {
                        *active = None;
                    }
                }
                return true;
            }
        }
        false
    }

    pub fn set_active(&self, id: &WorkingSetId, now_ms: u64) -> bool {
        if let Ok(mut sets) = self.sets.write() {
            if sets.contains_key(id) {
                if let Ok(mut active) = self.active_set.write() {
                    *active = Some(*id);
                }
                if let Some(ws) = sets.get_mut(id) {
                    ws.last_accessed_at = now_ms;
                    ws.access_count = ws.access_count.saturating_add(1);
                }
                return true;
            }
        }
        false
    }

    pub fn get_active(&self) -> Option<WorkingSetId> {
        *self.active_set.read().ok()?
    }

    pub fn clear_active(&self) {
        if let Ok(mut active) = self.active_set.write() {
            *active = None;
        }
    }

    pub fn resolve_documents(
        &self,
        id: &WorkingSetId,
        all_documents: &[(String, String, HashMap<String, serde_json::Value>)],
    ) -> Vec<(String, String)> {
        let ws = match self.get_set(id) {
            Some(ws) => ws,
            None => return Vec::new(),
        };

        all_documents
            .iter()
            .filter(|(_, _, fields)| ws.filter.matches(fields))
            .map(|(doc_id, record_id, _)| (doc_id.clone(), record_id.clone()))
            .collect()
    }

    pub fn prefetch_candidates(
        &self,
        budget_bytes: u64,
        all_documents: &[(String, String, u64, HashMap<String, serde_json::Value>)],
    ) -> Vec<(String, String, u64)> {
        let active_id = match self.get_active() {
            Some(id) => id,
            None => return Vec::new(),
        };

        let ws = match self.get_set(&active_id) {
            Some(ws) => ws,
            None => return Vec::new(),
        };

        let mut candidates: Vec<(String, String, u64)> = all_documents
            .iter()
            .filter(|(_, _, _, fields)| ws.filter.matches(fields))
            .map(|(doc_id, record_id, size, _)| (doc_id.clone(), record_id.clone(), *size))
            .collect();

        candidates.sort_by(|a, b| a.2.cmp(&b.2));

        let mut selected = Vec::new();
        let mut total_bytes = 0u64;
        for (doc_id, record_id, size) in candidates {
            if total_bytes + size > budget_bytes {
                break;
            }
            total_bytes += size;
            selected.push((doc_id, record_id, size));
        }

        selected
    }

    pub fn effective_score(
        &self,
        _doc_id: &str,
        _record_id: &str,
        base_score: f64,
    ) -> f64 {
        let active_id = match self.get_active() {
            Some(id) => id,
            None => return base_score,
        };

        let ws = match self.get_set(&active_id) {
            Some(ws) => ws,
            None => return base_score,
        };

        let affinity = if ws.workspace_id.is_some() {
            1.2
        } else {
            1.0
        };

        base_score * affinity
    }

    pub fn count(&self) -> usize {
        self.sets.read().map(|s| s.len()).unwrap_or(0)
    }
}

impl Default for WorkingSetManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_filter() -> QueryFilter {
        QueryFilter::new().with_status("OPEN")
    }

    #[test]
    fn test_create_working_set() {
        let mgr = WorkingSetManager::new();
        let id = mgr.create_set("Open Invoices", test_filter(), WorkingSetPolicy::Manual, Some(1), 1000);

        let ws = mgr.get_set(&id).unwrap();
        assert_eq!(ws.name, "Open Invoices");
        assert_eq!(ws.workspace_id, Some(1));
        assert_eq!(ws.created_at, 1000);
    }

    #[test]
    fn test_list_working_sets() {
        let mgr = WorkingSetManager::new();
        mgr.create_set("Open Invoices", test_filter(), WorkingSetPolicy::Manual, Some(1), 1000);
        mgr.create_set("Recent Payments", QueryFilter::new(), WorkingSetPolicy::Manual, Some(1), 2000);

        let list = mgr.list_sets();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_update_working_set() {
        let mgr = WorkingSetManager::new();
        let id = mgr.create_set("Open Invoices", test_filter(), WorkingSetPolicy::Manual, Some(1), 1000);

        let updated = mgr.update_set(&id, Some("Open Invoices v2"), None, None, 2000);
        assert!(updated);

        let ws = mgr.get_set(&id).unwrap();
        assert_eq!(ws.name, "Open Invoices v2");
        assert_eq!(ws.updated_at, 2000);
    }

    #[test]
    fn test_delete_working_set() {
        let mgr = WorkingSetManager::new();
        let id = mgr.create_set("Open Invoices", test_filter(), WorkingSetPolicy::Manual, Some(1), 1000);

        assert!(mgr.delete_set(&id));
        assert!(mgr.get_set(&id).is_none());
    }

    #[test]
    fn test_set_active() {
        let mgr = WorkingSetManager::new();
        let id = mgr.create_set("Open Invoices", test_filter(), WorkingSetPolicy::Manual, Some(1), 1000);

        assert!(mgr.set_active(&id, 2000));
        assert_eq!(mgr.get_active(), Some(id));

        let ws = mgr.get_set(&id).unwrap();
        assert_eq!(ws.last_accessed_at, 2000);
        assert_eq!(ws.access_count, 1);
    }

    #[test]
    fn test_clear_active() {
        let mgr = WorkingSetManager::new();
        let id = mgr.create_set("Open Invoices", test_filter(), WorkingSetPolicy::Manual, Some(1), 1000);

        mgr.set_active(&id, 2000);
        assert_eq!(mgr.get_active(), Some(id));

        mgr.clear_active();
        assert!(mgr.get_active().is_none());
    }

    #[test]
    fn test_resolve_documents() {
        let mgr = WorkingSetManager::new();
        let id = mgr.create_set("Open Invoices", QueryFilter::new().with_status("OPEN"), WorkingSetPolicy::Manual, Some(1), 1000);

        let mut doc1_fields = HashMap::new();
        doc1_fields.insert("status".to_string(), serde_json::json!("OPEN"));
        let mut doc2_fields = HashMap::new();
        doc2_fields.insert("status".to_string(), serde_json::json!("CLOSED"));

        let all_docs = vec![
            ("d1".to_string(), "r1".to_string(), doc1_fields),
            ("d2".to_string(), "r2".to_string(), doc2_fields),
        ];

        let resolved = mgr.resolve_documents(&id, &all_docs);
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].0, "d1");
    }

    #[test]
    fn test_prefetch_candidates() {
        let mgr = WorkingSetManager::new();
        let id = mgr.create_set("Open Invoices", QueryFilter::new().with_status("OPEN"), WorkingSetPolicy::Manual, Some(1), 1000);
        mgr.set_active(&id, 2000);

        let mut doc1_fields = HashMap::new();
        doc1_fields.insert("status".to_string(), serde_json::json!("OPEN"));
        let mut doc2_fields = HashMap::new();
        doc2_fields.insert("status".to_string(), serde_json::json!("CLOSED"));

        let all_docs = vec![
            ("d1".to_string(), "r1".to_string(), 1000u64, doc1_fields),
            ("d2".to_string(), "r2".to_string(), 2000u64, doc2_fields),
        ];

        let candidates = mgr.prefetch_candidates(5000, &all_docs);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].0, "d1");
    }

    #[test]
    fn test_working_set_count() {
        let mgr = WorkingSetManager::new();
        assert_eq!(mgr.count(), 0);

        mgr.create_set("A", test_filter(), WorkingSetPolicy::Manual, None, 1000);
        assert_eq!(mgr.count(), 1);

        mgr.create_set("B", test_filter(), WorkingSetPolicy::Manual, None, 2000);
        assert_eq!(mgr.count(), 2);
    }

    #[test]
    fn test_delete_active_clears_active() {
        let mgr = WorkingSetManager::new();
        let id = mgr.create_set("Open Invoices", test_filter(), WorkingSetPolicy::Manual, Some(1), 1000);
        mgr.set_active(&id, 2000);

        mgr.delete_set(&id);
        assert!(mgr.get_active().is_none());
    }

    #[test]
    fn test_auto_policy() {
        let policy = WorkingSetPolicy::Automatic(AutoPolicy {
            max_documents: 100,
            max_age_ms: 7 * 24 * 60 * 60 * 1000,
            eviction_strategy: EvictionStrategy::LeastRecentlyUsed,
        });

        let mgr = WorkingSetManager::new();
        let id = mgr.create_set("Auto Set", test_filter(), policy, None, 1000);
        let ws = mgr.get_set(&id).unwrap();

        match ws.policy {
            WorkingSetPolicy::Automatic(auto) => {
                assert_eq!(auto.max_documents, 100);
                assert_eq!(auto.max_age_ms, 7 * 24 * 60 * 60 * 1000);
            }
            _ => panic!("Expected Automatic policy"),
        }
    }
}
