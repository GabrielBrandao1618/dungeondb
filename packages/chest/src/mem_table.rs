use std::{collections::HashMap, mem};

use crate::value::Value;

type MemTableTable = HashMap<String, Value>;
pub struct MemTable {
    table: MemTableTable,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            table: HashMap::new(),
        }
    }
    pub fn set(&mut self, key: &str, value: Value) {
        self.table.insert(key.to_owned(), value);
    }
    pub fn get(&self, key: &str) -> Option<Value> {
        match self.table.get(key) {
            Some(found) => Some(found.clone()),
            None => None,
        }
    }
    pub fn flush(&mut self) -> MemTableTable {
        mem::replace(&mut self.table, HashMap::new())
    }
    pub fn size(&self) -> usize {
        self.table.len()
    }
}
