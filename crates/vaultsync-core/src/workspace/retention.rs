use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    pub max_age_ms: Option<u64>,
    pub max_size_bytes: Option<u64>,
    pub auto_archive: bool,
    pub tombstone_retention_ms: u64,
    pub max_versions: Option<u32>,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            max_age_ms: None,
            max_size_bytes: None,
            auto_archive: false,
            tombstone_retention_ms: 30 * 24 * 60 * 60 * 1000,
            max_versions: None,
        }
    }
}

impl RetentionPolicy {
    pub fn should_retain(&self, age_ms: u64) -> bool {
        match self.max_age_ms {
            Some(max_age) => age_ms <= max_age,
            None => true,
        }
    }

    pub fn should_archive(&self, age_ms: u64) -> bool {
        if !self.auto_archive {
            return false;
        }
        match self.max_age_ms {
            Some(max_age) => age_ms > max_age / 2,
            None => false,
        }
    }

    pub fn should_delete_tombstone(&self, tombstone_age_ms: u64) -> bool {
        tombstone_age_ms >= self.tombstone_retention_ms
    }

    pub fn max_age_description(&self) -> &str {
        match self.max_age_ms {
            Some(ms) => {
                let days = ms / (24 * 60 * 60 * 1000);
                if days > 0 {
                    "Retention: {} days"
                } else {
                    "Retention: {} hours"
                }
            }
            None => "No age limit",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_retention() {
        let policy = RetentionPolicy::default();
        assert!(policy.max_age_ms.is_none());
        assert!(policy.max_size_bytes.is_none());
        assert!(!policy.auto_archive);
        assert_eq!(policy.tombstone_retention_ms, 30 * 24 * 60 * 60 * 1000);
    }

    #[test]
    fn test_should_retain() {
        let policy = RetentionPolicy {
            max_age_ms: Some(30 * 24 * 60 * 60 * 1000),
            ..Default::default()
        };

        assert!(policy.should_retain(10 * 24 * 60 * 60 * 1000));
        assert!(policy.should_retain(30 * 24 * 60 * 60 * 1000));
        assert!(!policy.should_retain(31 * 24 * 60 * 60 * 1000));
    }

    #[test]
    fn test_should_archive() {
        let policy = RetentionPolicy {
            max_age_ms: Some(30 * 24 * 60 * 60 * 1000),
            auto_archive: true,
            ..Default::default()
        };

        assert!(!policy.should_archive(5 * 24 * 60 * 60 * 1000));
        assert!(policy.should_archive(20 * 24 * 60 * 60 * 1000));
        assert!(policy.should_archive(35 * 24 * 60 * 60 * 1000));
    }

    #[test]
    fn test_should_delete_tombstone() {
        let policy = RetentionPolicy {
            tombstone_retention_ms: 7 * 24 * 60 * 60 * 1000,
            ..Default::default()
        };

        assert!(!policy.should_delete_tombstone(5 * 24 * 60 * 60 * 1000));
        assert!(policy.should_delete_tombstone(7 * 24 * 60 * 60 * 1000));
        assert!(policy.should_delete_tombstone(10 * 24 * 60 * 60 * 1000));
    }

    #[test]
    fn test_no_age_limit() {
        let policy = RetentionPolicy::default();
        assert!(policy.should_retain(u64::MAX));
    }
}
