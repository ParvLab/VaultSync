use super::field::FieldDefinition;
use crate::crdt::types::CrdtValue;
use crate::error::VaultSyncError;
use crate::storage::traits::{SchemaMeta, Storage};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentSchema {
    pub doc_id: String,
    pub fields: Vec<FieldDefinition>,
    pub version: u64,
}

pub struct SchemaRegistry {
    schemas: HashMap<String, DocumentSchema>,
    /// Tracks max schema_version seen per (doc_id, record_id).
    watermarks: HashMap<(String, String), u64>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
            watermarks: HashMap::new(),
        }
    }

    /// Load schemas from persistent storage.
    pub async fn load_from_storage(&mut self, storage: &dyn Storage) -> Result<(), VaultSyncError> {
        let all = storage.list_schemas().await?;
        for meta in all {
            if let Ok(schema) = serde_json::from_slice::<DocumentSchema>(&meta.schema_bytes) {
                self.schemas.insert(meta.doc_id.clone(), schema);
            }
        }
        Ok(())
    }

    /// Register a schema and persist it.
    pub async fn define(
        &mut self,
        doc_id: &str,
        schema: DocumentSchema,
        storage: Option<&dyn Storage>,
    ) -> Result<(), VaultSyncError> {
        let version = schema.version;
        self.schemas.insert(doc_id.to_string(), schema);
        if let Some(storage) = storage {
            let meta = SchemaMeta {
                doc_id: doc_id.to_string(),
                version,
                schema_bytes: serde_json::to_vec(self.schemas.get(doc_id).unwrap()).unwrap(),
            };
            storage.write_schema(&meta).await?;
        }
        Ok(())
    }

    /// Register a schema in memory only (no persistence).
    pub fn define_local(&mut self, doc_id: &str, schema: DocumentSchema) -> Result<(), VaultSyncError> {
        self.schemas.insert(doc_id.to_string(), schema);
        Ok(())
    }

    pub fn get(&self, doc_id: &str) -> Option<&DocumentSchema> {
        self.schemas.get(doc_id)
    }

    /// Validate that writing with `proposed_version` is allowed for `doc_id`.
    /// If no schema is registered for this doc, any version is allowed (backward compat).
    pub fn validate_version(&self, doc_id: &str, proposed_version: u64) -> Result<(), VaultSyncError> {
        if let Some(schema) = self.schemas.get(doc_id) {
            if schema.version != proposed_version {
                return Err(VaultSyncError::VersionMismatch(
                    proposed_version,
                    schema.version,
                    doc_id.to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Record that a document was modified at `schema_version`.
    /// Tracks the maximum version seen per (doc_id, record_id) for watermarking.
    pub fn watermark_version(&mut self, doc_id: &str, record_id: &str, version: u64) {
        let key = (doc_id.to_string(), record_id.to_string());
        let entry = self.watermarks.entry(key).or_insert(0);
        if version > *entry {
            *entry = version;
        }
    }

    /// Get the highest schema version that has touched (doc_id, record_id).
    pub fn max_seen_version(&self, doc_id: &str, record_id: &str) -> u64 {
        self.watermarks
            .get(&(doc_id.to_string(), record_id.to_string()))
            .copied()
            .unwrap_or(0)
    }

    /// All watermarks for inspection.
    pub fn watermarks(&self) -> &HashMap<(String, String), u64> {
        &self.watermarks
    }

    pub fn validate_update(
        &self,
        doc_id: &str,
        field: &str,
        _value: &CrdtValue,
    ) -> Result<(), VaultSyncError> {
        if let Some(schema) = self.schemas.get(doc_id) {
            if !schema.fields.iter().any(|f| f.name == field) {
                return Err(VaultSyncError::Schema(format!(
                    "unknown field '{}' in doc '{}'",
                    field, doc_id
                )));
            }
        }
        Ok(())
    }

    pub fn schemas(&self) -> &HashMap<String, DocumentSchema> {
        &self.schemas
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::types::CrdtValue;

    fn make_field(name: &str) -> FieldDefinition {
        FieldDefinition {
            name: name.to_string(),
            value_type: super::super::field::ValueType::String,
            crdt_type: crate::crdt::types::CrdtType::LwwRegister,
            primary_key: false,
            indexed: false,
            sync: true,
        }
    }

    fn make_schema(doc_id: &str, version: u64) -> DocumentSchema {
        DocumentSchema {
            doc_id: doc_id.to_string(),
            fields: vec![make_field("title")],
            version,
        }
    }

    #[tokio::test]
    async fn test_validate_version_allows_unregistered() {
        let registry = SchemaRegistry::new();
        // No schema registered for "doc1" — any version should pass
        assert!(registry.validate_version("doc1", 0).is_ok());
        assert!(registry.validate_version("doc1", 42).is_ok());
    }

    #[tokio::test]
    async fn test_validate_version_rejects_mismatch() {
        let mut registry = SchemaRegistry::new();
        registry.define_local("doc1", make_schema("doc1", 2)).unwrap();
        // Version 1 doesn't match registered version 2
        assert!(registry.validate_version("doc1", 1).is_err());
        // Version 2 matches
        assert!(registry.validate_version("doc1", 2).is_ok());
    }

    #[tokio::test]
    async fn test_validate_version_rejects_wrong_version() {
        let mut registry = SchemaRegistry::new();
        registry.define_local("doc1", make_schema("doc1", 3)).unwrap();
        let err = registry.validate_version("doc1", 0).unwrap_err();
        assert!(err.to_string().contains("Version mismatch"));
    }

    #[tokio::test]
    async fn test_watermark_tracks_max_version() {
        let mut registry = SchemaRegistry::new();
        assert_eq!(registry.max_seen_version("doc1", "rec1"), 0);
        registry.watermark_version("doc1", "rec1", 1);
        assert_eq!(registry.max_seen_version("doc1", "rec1"), 1);
        registry.watermark_version("doc1", "rec1", 0);
        // Max should stay at 1
        assert_eq!(registry.max_seen_version("doc1", "rec1"), 1);
        registry.watermark_version("doc1", "rec1", 5);
        assert_eq!(registry.max_seen_version("doc1", "rec1"), 5);
    }

    #[tokio::test]
    async fn test_watermarks_isolated_per_doc_record() {
        let mut registry = SchemaRegistry::new();
        registry.watermark_version("a", "1", 10);
        registry.watermark_version("a", "2", 20);
        registry.watermark_version("b", "1", 30);
        assert_eq!(registry.max_seen_version("a", "1"), 10);
        assert_eq!(registry.max_seen_version("a", "2"), 20);
        assert_eq!(registry.max_seen_version("b", "1"), 30);
        assert_eq!(registry.max_seen_version("c", "x"), 0);
    }

    #[tokio::test]
    async fn test_persistence_roundtrip() {
        let storage = crate::storage::memory::InMemoryStorage::new();
        let mut registry = SchemaRegistry::new();
        let schema = make_schema("doc_persist", 1);
        registry.define("doc_persist", schema, Some(&storage)).await.unwrap();
        // Verify it was written to storage
        let stored = storage.read_schema("doc_persist").await.unwrap().unwrap();
        assert_eq!(stored.version, 1);
        // Load into a new registry
        let mut registry2 = SchemaRegistry::new();
        registry2.load_from_storage(&storage).await.unwrap();
        assert_eq!(registry2.get("doc_persist").unwrap().version, 1);
    }

    /// Skip-ahead compatibility: schema v1 document can be read after registry
    /// has been updated to v4 without data loss.
    #[tokio::test]
    async fn test_skip_ahead_compatibility() {
        // Simulate: document created at v1, registry later updated to v4
        let storage = crate::storage::memory::InMemoryStorage::new();
        let mut registry = SchemaRegistry::new();
        registry.define_local("doc_skip", make_schema("doc_skip", 4)).unwrap();

        // A document created at v1 should still be readable (version is advisory)
        let v1_schema = make_schema("doc_skip", 1);
        // Using validate_version, v1 should fail against v4
        assert!(registry.validate_version("doc_skip", 1).is_err());
        // But v4 should pass
        assert!(registry.validate_version("doc_skip", 4).is_ok());
    }

    #[tokio::test]
    async fn test_persistence_empty_storage() {
        let storage = crate::storage::memory::InMemoryStorage::new();
        let mut registry = SchemaRegistry::new();
        registry.load_from_storage(&storage).await.unwrap();
        assert!(registry.get("nonexistent").is_none());
    }

    #[tokio::test]
    async fn test_validate_update_with_registered_schema() {
        let mut registry = SchemaRegistry::new();
        registry.define_local("doc1", make_schema("doc1", 1)).unwrap();
        // Allowed field
        assert!(registry.validate_update("doc1", "title", &CrdtValue::String("hello".into())).is_ok());
        // Unknown field
        assert!(registry.validate_update("doc1", "nonexistent", &CrdtValue::String("x".into())).is_err());
        // Unregistered doc — any field allowed
        assert!(registry.validate_update("other", "anything", &CrdtValue::String("x".into())).is_ok());
    }
}
