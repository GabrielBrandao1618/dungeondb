use std::time::UNIX_EPOCH;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
    Invalid,
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
