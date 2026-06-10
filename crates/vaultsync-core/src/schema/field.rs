use crate::crdt::types::CrdtType;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDefinition {
    pub name: String,
    pub value_type: ValueType,
    pub crdt_type: CrdtType,
    pub primary_key: bool,
    pub indexed: bool,
    pub sync: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValueType {
    String,
    Number,
    Boolean,
    Array,
    Object,
}
