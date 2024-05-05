use std::{collections::BTreeMap, mem};

use crate::value::TimeStampedValue;

type MemTableTable = BTreeMap<String, TimeStampedValue>;
pub struct MemTable {
    table: MemTableTable,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            table: Default::default(),
        }
    }
    pub fn set(&mut self, key: &str, value: TimeStampedValue) {
        self.table.insert(key.to_owned(), value);
    }
    pub fn get(&self, key: &str) -> Option<TimeStampedValue> {
        self.table.get(key).cloned()
    }
    pub fn flush(&mut self) -> MemTableTable {
        mem::take(&mut self.table)
    }
    pub fn size(&self) -> usize {
        self.table.len()
    }
}
