use std::time::UNIX_EPOCH;

use query::ast::Literal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Invalid,
}

impl Value {
    pub fn to_query(self) -> Literal {
        match self {
            Value::Integer(v) => Literal::Integer(v),
            Value::Float(v) => Literal::Float(v),
            Value::String(v) => Literal::String(v),
            Value::Boolean(v) => Literal::Boolean(v),
            Value::Invalid => unreachable!(),
        }
    }
    pub fn from_query_literal(lit: Literal) -> Self {
        match lit {
            Literal::String(v) => Self::String(v),
            Literal::Integer(v) => Self::Integer(v),
            Literal::Float(v) => Self::Float(v),
            Literal::Boolean(v) => Self::Boolean(v),
            Literal::Null => Self::Invalid,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TimeStampedValue {
    pub timestamp: u128,
    pub value: Value,
}

impl TimeStampedValue {
    pub fn new(value: Value) -> Self {
        let current_time = std::time::SystemTime::now();
        let ellapsed = current_time.duration_since(UNIX_EPOCH).unwrap();
        Self {
            timestamp: ellapsed.as_nanos(),
            value,
        }
    }
    pub fn to_query(self) -> Literal {
        self.value.to_query()
    }
}

impl Ord for TimeStampedValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}
impl PartialOrd for TimeStampedValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.timestamp.cmp(&other.timestamp))
    }
}
impl Eq for TimeStampedValue {}
