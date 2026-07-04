use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryFilter {
    pub status: Option<String>,
    pub workspace_id: Option<u64>,
    pub tags: Vec<String>,
    pub min_updated_at: Option<u64>,
    pub max_updated_at: Option<u64>,
    pub custom_filters: HashMap<String, serde_json::Value>,
}

impl QueryFilter {
    pub fn new() -> Self {
        Self {
            status: None,
            workspace_id: None,
            tags: Vec::new(),
            min_updated_at: None,
            max_updated_at: None,
            custom_filters: HashMap::new(),
        }
    }

    pub fn with_status(mut self, status: &str) -> Self {
        self.status = Some(status.to_string());
        self
    }

    pub fn with_workspace_id(mut self, workspace_id: u64) -> Self {
        self.workspace_id = Some(workspace_id);
        self
    }

    pub fn with_tag(mut self, tag: &str) -> Self {
        self.tags.push(tag.to_string());
        self
    }

    pub fn with_min_updated_at(mut self, timestamp: u64) -> Self {
        self.min_updated_at = Some(timestamp);
        self
    }

    pub fn with_max_updated_at(mut self, timestamp: u64) -> Self {
        self.max_updated_at = Some(timestamp);
        self
    }

    pub fn with_custom_filter(mut self, key: &str, value: serde_json::Value) -> Self {
        self.custom_filters.insert(key.to_string(), value);
        self
    }

    pub fn matches(&self, fields: &HashMap<String, serde_json::Value>) -> bool {
        if let Some(ref status) = self.status {
            match fields.get("status") {
                Some(serde_json::Value::String(s)) if s == status => {}
                _ => return false,
            }
        }

        if let Some(min_ts) = self.min_updated_at {
            match fields.get("updated_at") {
                Some(serde_json::Value::Number(n)) => {
                    if let Some(v) = n.as_u64() {
                        if v < min_ts {
                            return false;
                        }
                    }
                }
                _ => return false,
            }
        }

        if let Some(max_ts) = self.max_updated_at {
            match fields.get("updated_at") {
                Some(serde_json::Value::Number(n)) => {
                    if let Some(v) = n.as_u64() {
                        if v > max_ts {
                            return false;
                        }
                    }
                }
                _ => return false,
            }
        }

        if !self.tags.is_empty() {
            match fields.get("tags") {
                Some(serde_json::Value::Array(tags)) => {
                    for required_tag in &self.tags {
                        if !tags.iter().any(|t| t.as_str() == Some(required_tag.as_str())) {
                            return false;
                        }
                    }
                }
                _ => return false,
            }
        }

        for (key, expected) in &self.custom_filters {
            match fields.get(key) {
                Some(actual) if actual == expected => {}
                _ => return false,
            }
        }

        true
    }
}

impl Default for QueryFilter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_filter_matches_all() {
        let filter = QueryFilter::new();
        let fields = HashMap::new();
        assert!(filter.matches(&fields));
    }

    #[test]
    fn test_status_filter() {
        let filter = QueryFilter::new().with_status("OPEN");

        let mut fields = HashMap::new();
        fields.insert("status".to_string(), serde_json::json!("OPEN"));
        assert!(filter.matches(&fields));

        let mut fields = HashMap::new();
        fields.insert("status".to_string(), serde_json::json!("CLOSED"));
        assert!(!filter.matches(&fields));
    }

    #[test]
    fn test_min_updated_at_filter() {
        let filter = QueryFilter::new().with_min_updated_at(1000);

        let mut fields = HashMap::new();
        fields.insert("updated_at".to_string(), serde_json::json!(2000));
        assert!(filter.matches(&fields));

        let mut fields = HashMap::new();
        fields.insert("updated_at".to_string(), serde_json::json!(500));
        assert!(!filter.matches(&fields));
    }

    #[test]
    fn test_max_updated_at_filter() {
        let filter = QueryFilter::new().with_max_updated_at(1000);

        let mut fields = HashMap::new();
        fields.insert("updated_at".to_string(), serde_json::json!(500));
        assert!(filter.matches(&fields));

        let mut fields = HashMap::new();
        fields.insert("updated_at".to_string(), serde_json::json!(2000));
        assert!(!filter.matches(&fields));
    }

    #[test]
    fn test_tag_filter() {
        let filter = QueryFilter::new().with_tag("important");

        let mut fields = HashMap::new();
        fields.insert("tags".to_string(), serde_json::json!(["important", "draft"]));
        assert!(filter.matches(&fields));

        let mut fields = HashMap::new();
        fields.insert("tags".to_string(), serde_json::json!(["draft"]));
        assert!(!filter.matches(&fields));
    }

    #[test]
    fn test_custom_filter() {
        let filter = QueryFilter::new()
            .with_custom_filter("company", serde_json::json!("ACME"));

        let mut fields = HashMap::new();
        fields.insert("company".to_string(), serde_json::json!("ACME"));
        assert!(filter.matches(&fields));

        let mut fields = HashMap::new();
        fields.insert("company".to_string(), serde_json::json!("Other"));
        assert!(!filter.matches(&fields));
    }

    #[test]
    fn test_combined_filters() {
        let filter = QueryFilter::new()
            .with_status("OPEN")
            .with_tag("urgent");

        let mut fields = HashMap::new();
        fields.insert("status".to_string(), serde_json::json!("OPEN"));
        fields.insert("tags".to_string(), serde_json::json!(["urgent"]));
        assert!(filter.matches(&fields));

        let mut fields = HashMap::new();
        fields.insert("status".to_string(), serde_json::json!("OPEN"));
        fields.insert("tags".to_string(), serde_json::json!(["normal"]));
        assert!(!filter.matches(&fields));
    }

    #[test]
    fn test_missing_status_field() {
        let filter = QueryFilter::new().with_status("OPEN");
        let fields = HashMap::new();
        assert!(!filter.matches(&fields));
    }

    #[test]
    fn test_missing_updated_at_field() {
        let filter = QueryFilter::new().with_min_updated_at(1000);
        let fields = HashMap::new();
        assert!(!filter.matches(&fields));
    }

    #[test]
    fn test_missing_tags_field() {
        let filter = QueryFilter::new().with_tag("important");
        let fields = HashMap::new();
        assert!(!filter.matches(&fields));
    }
}
