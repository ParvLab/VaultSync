use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;

pub mod schema;
pub mod replication;
pub mod retention;

pub use schema::WorkspaceSchema;
pub use replication::ReplicationPolicy;
pub use retention::RetentionPolicy;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkspaceId(pub u64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workspace {
    pub id: WorkspaceId,
    pub name: String,
    pub namespace: String,
    pub schema: WorkspaceSchema,
    pub replication_policy: ReplicationPolicy,
    pub retention_policy: RetentionPolicy,
    pub created_at: u64,
    pub updated_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceInfo {
    pub id: WorkspaceId,
    pub name: String,
    pub namespace: String,
    pub created_at: u64,
}

impl From<&Workspace> for WorkspaceInfo {
    fn from(w: &Workspace) -> Self {
        Self {
            id: w.id,
            name: w.name.clone(),
            namespace: w.namespace.clone(),
            created_at: w.created_at,
        }
    }
}

pub struct WorkspaceManager {
    workspaces: RwLock<HashMap<WorkspaceId, Workspace>>,
    next_id: RwLock<u64>,
}

impl std::fmt::Debug for WorkspaceManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkspaceManager")
            .field("count", &self.count())
            .finish()
    }
}

impl WorkspaceManager {
    pub fn new() -> Self {
        Self {
            workspaces: RwLock::new(HashMap::new()),
            next_id: RwLock::new(1),
        }
    }

    pub fn create_workspace(
        &self,
        name: &str,
        namespace: &str,
        schema: WorkspaceSchema,
        replication_policy: ReplicationPolicy,
        retention_policy: RetentionPolicy,
        now_ms: u64,
    ) -> WorkspaceId {
        let id = {
            let mut next = self.next_id.write().unwrap();
            let id = WorkspaceId(*next);
            *next += 1;
            id
        };

        let workspace = Workspace {
            id,
            name: name.to_string(),
            namespace: namespace.to_string(),
            schema,
            replication_policy,
            retention_policy,
            created_at: now_ms,
            updated_at: now_ms,
        };

        if let Ok(mut workspaces) = self.workspaces.write() {
            workspaces.insert(id, workspace);
        }

        id
    }

    pub fn get_workspace(&self, id: &WorkspaceId) -> Option<Workspace> {
        self.workspaces.read().ok()?.get(id).cloned()
    }

    pub fn list_workspaces(&self) -> Vec<WorkspaceInfo> {
        match self.workspaces.read() {
            Ok(workspaces) => workspaces.values().map(WorkspaceInfo::from).collect(),
            Err(_) => Vec::new(),
        }
    }

    pub fn update_workspace(
        &self,
        id: &WorkspaceId,
        name: Option<&str>,
        schema: Option<WorkspaceSchema>,
        replication_policy: Option<ReplicationPolicy>,
        retention_policy: Option<RetentionPolicy>,
        now_ms: u64,
    ) -> bool {
        if let Ok(mut workspaces) = self.workspaces.write() {
            if let Some(workspace) = workspaces.get_mut(id) {
                if let Some(n) = name {
                    workspace.name = n.to_string();
                }
                if let Some(s) = schema {
                    workspace.schema = s;
                }
                if let Some(r) = replication_policy {
                    workspace.replication_policy = r;
                }
                if let Some(r) = retention_policy {
                    workspace.retention_policy = r;
                }
                workspace.updated_at = now_ms;
                return true;
            }
        }
        false
    }

    pub fn delete_workspace(&self, id: &WorkspaceId) -> bool {
        if let Ok(mut workspaces) = self.workspaces.write() {
            workspaces.remove(id).is_some()
        } else {
            false
        }
    }

    pub fn get_workspace_for_namespace(&self, namespace: &str) -> Option<Workspace> {
        match self.workspaces.read() {
            Ok(workspaces) => workspaces
                .values()
                .find(|w| w.namespace == namespace)
                .cloned(),
            Err(_) => None,
        }
    }

    pub fn replication_scope(&self, id: &WorkspaceId) -> Option<ReplicationPolicy> {
        self.workspaces
            .read()
            .ok()?
            .get(id)
            .map(|w| w.replication_policy.clone())
    }

    pub fn retention_policy(&self, id: &WorkspaceId) -> Option<RetentionPolicy> {
        self.workspaces
            .read()
            .ok()?
            .get(id)
            .map(|w| w.retention_policy.clone())
    }

    pub fn count(&self) -> usize {
        self.workspaces.read().map(|w| w.len()).unwrap_or(0)
    }
}

impl Default for WorkspaceManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> WorkspaceSchema {
        WorkspaceSchema::new()
    }

    fn test_replication() -> ReplicationPolicy {
        ReplicationPolicy::FullSync
    }

    fn test_retention() -> RetentionPolicy {
        RetentionPolicy::default()
    }

    #[test]
    fn test_create_workspace() {
        let mgr = WorkspaceManager::new();
        let id = mgr.create_workspace(
            "Finance",
            "finance",
            test_schema(),
            test_replication(),
            test_retention(),
            1000,
        );
        assert_eq!(id, WorkspaceId(1));

        let ws = mgr.get_workspace(&id).unwrap();
        assert_eq!(ws.name, "Finance");
        assert_eq!(ws.namespace, "finance");
        assert_eq!(ws.created_at, 1000);
    }

    #[test]
    fn test_list_workspaces() {
        let mgr = WorkspaceManager::new();
        mgr.create_workspace("Finance", "finance", test_schema(), test_replication(), test_retention(), 1000);
        mgr.create_workspace("HR", "hr", test_schema(), test_replication(), test_retention(), 2000);

        let list = mgr.list_workspaces();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_update_workspace() {
        let mgr = WorkspaceManager::new();
        let id = mgr.create_workspace("Finance", "finance", test_schema(), test_replication(), test_retention(), 1000);

        let updated = mgr.update_workspace(&id, Some("Finance v2"), None, None, None, 2000);
        assert!(updated);

        let ws = mgr.get_workspace(&id).unwrap();
        assert_eq!(ws.name, "Finance v2");
        assert_eq!(ws.updated_at, 2000);
    }

    #[test]
    fn test_delete_workspace() {
        let mgr = WorkspaceManager::new();
        let id = mgr.create_workspace("Finance", "finance", test_schema(), test_replication(), test_retention(), 1000);

        assert!(mgr.delete_workspace(&id));
        assert!(mgr.get_workspace(&id).is_none());
    }

    #[test]
    fn test_get_workspace_for_namespace() {
        let mgr = WorkspaceManager::new();
        mgr.create_workspace("Finance", "finance", test_schema(), test_replication(), test_retention(), 1000);

        let ws = mgr.get_workspace_for_namespace("finance");
        assert!(ws.is_some());
        assert_eq!(ws.unwrap().name, "Finance");

        assert!(mgr.get_workspace_for_namespace("nonexistent").is_none());
    }

    #[test]
    fn test_replication_scope() {
        let mgr = WorkspaceManager::new();
        let id = mgr.create_workspace(
            "Finance",
            "finance",
            test_schema(),
            ReplicationPolicy::ReadOnly,
            test_retention(),
            1000,
        );

        let scope = mgr.replication_scope(&id).unwrap();
        assert_eq!(scope, ReplicationPolicy::ReadOnly);
    }

    #[test]
    fn test_workspace_count() {
        let mgr = WorkspaceManager::new();
        assert_eq!(mgr.count(), 0);

        mgr.create_workspace("Finance", "finance", test_schema(), test_replication(), test_retention(), 1000);
        assert_eq!(mgr.count(), 1);

        mgr.create_workspace("HR", "hr", test_schema(), test_replication(), test_retention(), 2000);
        assert_eq!(mgr.count(), 2);
    }

    #[test]
    fn test_next_id_increments() {
        let mgr = WorkspaceManager::new();
        let id1 = mgr.create_workspace("A", "a", test_schema(), test_replication(), test_retention(), 1000);
        let id2 = mgr.create_workspace("B", "b", test_schema(), test_replication(), test_retention(), 2000);
        let id3 = mgr.create_workspace("C", "c", test_schema(), test_replication(), test_retention(), 3000);

        assert_eq!(id1, WorkspaceId(1));
        assert_eq!(id2, WorkspaceId(2));
        assert_eq!(id3, WorkspaceId(3));
    }
}
