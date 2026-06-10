use std::collections::HashMap;
use crate::error::VaultSyncError;
use crate::crdt::types::CrdtValue;
use super::field::FieldDefinition;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentSchema {
    pub doc_id: String,
    pub fields: Vec<FieldDefinition>,
    pub version: u64,
}

pub struct SchemaRegistry {
    schemas: HashMap<String, DocumentSchema>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self { schemas: HashMap::new() }
    }

    pub fn define(&mut self, doc_id: &str, schema: DocumentSchema) -> Result<(), VaultSyncError> {
        self.schemas.insert(doc_id.to_string(), schema);
        Ok(())
    }

    pub fn get(&self, doc_id: &str) -> Option<&DocumentSchema> {
        self.schemas.get(doc_id)
    }

    pub fn validate_update(&self, doc_id: &str, field: &str, _value: &CrdtValue) -> Result<(), VaultSyncError> {
        if let Some(schema) = self.schemas.get(doc_id) {
            if !schema.fields.iter().any(|f| f.name == field) {
                return Err(VaultSyncError::Schema(format!("unknown field '{}' in doc '{}'", field, doc_id)));
            }
        }
        Ok(())
    }

    pub fn schemas(&self) -> &HashMap<String, DocumentSchema> {
        &self.schemas
    }
}
