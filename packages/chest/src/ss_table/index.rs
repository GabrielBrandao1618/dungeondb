use std::{collections::BTreeMap, path::PathBuf};

use errors::{DungeonError, DungeonResult};
use rmp_serde::from_read;
use serde::{Deserialize, Serialize};

use super::DocumentSegment;

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct Index {
    pub table: BTreeMap<String, DocumentSegment>,
}
impl Index {
    pub fn new() -> Self {
        Self {
            table: BTreeMap::new(),
        }
    }
    pub fn from_file(file_path: PathBuf) -> DungeonResult<Self> {
        let parsed_index: Self = from_read(
            std::fs::File::open(file_path)
                .map_err(|_| DungeonError::new("Could not open index file"))?,
        )
        .map_err(|_| DungeonError::new("Could not parse index file"))?;
        Ok(parsed_index)
    }
    pub fn insert(&mut self, key: String, segment: DocumentSegment) {
        self.table.insert(key, segment);
    }
    pub fn get(&self, key: &str) -> Option<DocumentSegment> {
        let found = self.table.get(key).cloned();
        found
    }
}
impl Iterator for Index {
    type Item = (String, DocumentSegment);

    fn next(&mut self) -> Option<Self::Item> {
        self.table.pop_first()
    }
}
