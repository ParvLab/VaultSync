use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationPolicy {
    FullSync,
    Selective(Vec<String>),
    ReadOnly,
    Offline,
}

impl ReplicationPolicy {
    pub fn should_sync(&self, document_type: &str) -> bool {
        match self {
            ReplicationPolicy::FullSync => true,
            ReplicationPolicy::Selective(types) => types.contains(&document_type.to_string()),
            ReplicationPolicy::ReadOnly => false,
            ReplicationPolicy::Offline => false,
        }
    }

    pub fn allows_upload(&self) -> bool {
        match self {
            ReplicationPolicy::FullSync => true,
            ReplicationPolicy::Selective(_) => true,
            ReplicationPolicy::ReadOnly => false,
            ReplicationPolicy::Offline => false,
        }
    }

    pub fn allows_download(&self) -> bool {
        match self {
            ReplicationPolicy::FullSync => true,
            ReplicationPolicy::Selective(_) => true,
            ReplicationPolicy::ReadOnly => true,
            ReplicationPolicy::Offline => false,
        }
    }

    pub fn description(&self) -> &str {
        match self {
            ReplicationPolicy::FullSync => "Full bidirectional sync",
            ReplicationPolicy::Selective(_) => "Selective sync for specific document types",
            ReplicationPolicy::ReadOnly => "Download only, no uploads",
            ReplicationPolicy::Offline => "No sync, offline only",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_sync() {
        let policy = ReplicationPolicy::FullSync;
        assert!(policy.should_sync("invoice"));
        assert!(policy.should_sync("customer"));
        assert!(policy.allows_upload());
        assert!(policy.allows_download());
    }

    #[test]
    fn test_selective_sync() {
        let policy = ReplicationPolicy::Selective(vec!["invoice".into(), "payment".into()]);
        assert!(policy.should_sync("invoice"));
        assert!(policy.should_sync("payment"));
        assert!(!policy.should_sync("customer"));
        assert!(policy.allows_upload());
        assert!(policy.allows_download());
    }

    #[test]
    fn test_read_only() {
        let policy = ReplicationPolicy::ReadOnly;
        assert!(!policy.should_sync("invoice"));
        assert!(!policy.allows_upload());
        assert!(policy.allows_download());
    }

    #[test]
    fn test_offline() {
        let policy = ReplicationPolicy::Offline;
        assert!(!policy.should_sync("invoice"));
        assert!(!policy.allows_upload());
        assert!(!policy.allows_download());
    }

    #[test]
    fn test_descriptions() {
        assert_eq!(ReplicationPolicy::FullSync.description(), "Full bidirectional sync");
        assert_eq!(ReplicationPolicy::ReadOnly.description(), "Download only, no uploads");
        assert_eq!(ReplicationPolicy::Offline.description(), "No sync, offline only");
    }
}
