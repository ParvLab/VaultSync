use crate::crdt::types::CrdtValue;

#[derive(Debug, Clone)]
pub enum Filter {
    FieldEquals { field: String, value: CrdtValue },
    FieldContains { field: String, value: CrdtValue },
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
}

impl Filter {
    pub fn matches(&self, doc: &std::collections::HashMap<String, CrdtValue>) -> bool {
        match self {
            Filter::FieldEquals { field, value } => {
                doc.get(field).map_or(false, |v| v == value)
            }
            Filter::FieldContains { field, value } => {
                doc.get(field).map_or(false, |v| match (v, value) {
                    (CrdtValue::String(hay), CrdtValue::String(needle)) => hay.contains(needle),
                    _ => false,
                })
            }
            Filter::And(filters) => filters.iter().all(|f| f.matches(doc)),
            Filter::Or(filters) => filters.iter().any(|f| f.matches(doc)),
            Filter::Not(filter) => !filter.matches(doc),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Filter::And(v) => v.is_empty(),
            _ => false,
        }
    }
}
