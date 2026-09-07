use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceSchema {
    pub fields: HashMap<String, FieldSchema>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSchema {
    pub field_type: FieldType,
    pub required: bool,
    pub indexed: bool,
    pub default_value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldType {
    String,
    Number,
    Boolean,
    Timestamp,
    Array,
    Object,
}

impl WorkspaceSchema {
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    pub fn add_field(&mut self, name: &str, field_type: FieldType, required: bool) {
        self.fields.insert(
            name.to_string(),
            FieldSchema {
                field_type,
                required,
                indexed: false,
                default_value: None,
            },
        );
    }

    pub fn add_indexed_field(&mut self, name: &str, field_type: FieldType, required: bool) {
        self.fields.insert(
            name.to_string(),
            FieldSchema {
                field_type,
                required,
                indexed: true,
                default_value: None,
            },
        );
    }

    pub fn get_field(&self, name: &str) -> Option<&FieldSchema> {
        self.fields.get(name)
    }

    pub fn field_names(&self) -> Vec<&str> {
        self.fields.keys().map(|s| s.as_str()).collect()
    }

    pub fn has_field(&self, name: &str) -> bool {
        self.fields.contains_key(name)
    }

    pub fn indexed_fields(&self) -> Vec<&str> {
        self.fields
            .iter()
            .filter(|(_, f)| f.indexed)
            .map(|(name, _)| name.as_str())
            .collect()
    }

    pub fn validate_document(&self, doc: &HashMap<String, serde_json::Value>) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        for (name, field) in &self.fields {
            if field.required && !doc.contains_key(name) {
                errors.push(format!("Missing required field: {}", name));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

impl Default for WorkspaceSchema {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_schema() {
        let mut schema = WorkspaceSchema::new();
        schema.add_field("status", FieldType::String, true);
        schema.add_indexed_field("company", FieldType::String, false);
        schema.add_field("amount", FieldType::Number, false);

        assert_eq!(schema.field_names().len(), 3);
        assert!(schema.has_field("status"));
        assert!(schema.has_field("company"));
        assert!(schema.has_field("amount"));
    }

    #[test]
    fn test_indexed_fields() {
        let mut schema = WorkspaceSchema::new();
        schema.add_field("status", FieldType::String, true);
        schema.add_indexed_field("company", FieldType::String, false);
        schema.add_indexed_field("date", FieldType::Timestamp, false);

        let indexed = schema.indexed_fields();
        assert_eq!(indexed.len(), 2);
        assert!(indexed.contains(&"company"));
        assert!(indexed.contains(&"date"));
    }

    #[test]
    fn test_validate_document() {
        let mut schema = WorkspaceSchema::new();
        schema.add_field("status", FieldType::String, true);
        schema.add_field("company", FieldType::String, false);

        let mut doc = HashMap::new();
        doc.insert("status".to_string(), serde_json::json!("OPEN"));
        assert!(schema.validate_document(&doc).is_ok());

        let mut doc_missing = HashMap::new();
        doc_missing.insert("company".to_string(), serde_json::json!("ACME"));
        let errors = schema.validate_document(&doc_missing);
        assert!(errors.is_err());
        assert!(errors.unwrap_err().contains(&"Missing required field: status".to_string()));
    }
}
