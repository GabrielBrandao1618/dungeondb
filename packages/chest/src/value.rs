use std::time::UNIX_EPOCH;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TimeStampedValue {
    timestamp: u128,
    value: Value,
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
}
