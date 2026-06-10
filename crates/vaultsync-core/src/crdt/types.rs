use serde::{Serialize, Deserialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CrdtType {
    LwwRegister,
    PnCounter,
    OrSet,
    Text,
    Array,
}

impl CrdtType {
    pub fn as_str(&self) -> &'static str {
        match self {
            CrdtType::LwwRegister => "lww",
            CrdtType::PnCounter => "counter",
            CrdtType::OrSet => "orset",
            CrdtType::Text => "text",
            CrdtType::Array => "array",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CrdtValue {
    String(String),
    Number(f64),
    Boolean(bool),
    Array(Vec<CrdtValue>),
    Map(HashMap<String, CrdtValue>),
    Null,
}

impl CrdtValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            CrdtValue::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            CrdtValue::Number(n) => Some(*n),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            CrdtValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, CrdtValue::Null)
    }
}

impl From<String> for CrdtValue {
    fn from(s: String) -> Self { CrdtValue::String(s) }
}

impl From<&str> for CrdtValue {
    fn from(s: &str) -> Self { CrdtValue::String(s.to_string()) }
}

impl From<f64> for CrdtValue {
    fn from(n: f64) -> Self { CrdtValue::Number(n) }
}

impl From<bool> for CrdtValue {
    fn from(b: bool) -> Self { CrdtValue::Boolean(b) }
}
