use std::collections::HashMap;
use std::io;
use std::path::PathBuf;

use crate::value::Value;

use rmp_serde::encode::to_vec;
use serde::Deserialize;
use serde::Serialize;

#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct DocumentSegment {
    offset: usize,
    length: usize,
}
impl From<(usize, usize)> for DocumentSegment {
    fn from(value: (usize, usize)) -> Self {
        let (offset, length) = value;
        Self { offset, length }
    }
}

#[derive(Serialize, Deserialize)]
struct Index {
    table: HashMap<String, DocumentSegment>,
}
impl Index {
    pub fn new() -> Self {
        Self {
            table: HashMap::new(),
        }
    }
    pub fn _from_file(_file_path: &str) -> Self {
        Self {
            table: HashMap::new(),
        }
        // TODO actually read the file
    }
    pub fn insert(&mut self, key: String, segment: DocumentSegment) {
        self.table.insert(key, segment);
    }
}
pub struct SSTable {
    index: Index,
    content: Vec<u8>,
    base_dir: PathBuf,
}

impl SSTable {
    pub fn new(base_dir: PathBuf, table: HashMap<String, Value>) -> Self {
        let mut index = Index::new();
        let mut content = Vec::new();

        let mut current_offset = 0;
        for (key, value) in table {
            let serialized = to_vec(&value).unwrap();
            let serialized_length = serialized.len();
            index.insert(key, (current_offset, serialized_length).into());
            current_offset += serialized_length;
            content = [content, serialized].concat();
        }
        Self {
            base_dir,
            index,
            content,
        }
    }
    pub fn write(&self, file_name: &str) -> io::Result<()> {
        let index_path = self.base_dir.join(format!("{file_name}.index"));
        let data_path = self.base_dir.join(format!("{file_name}.chest"));

        std::fs::write(index_path, to_vec(&self.index).unwrap())?;
        std::fs::write(data_path, &self.content)?;
        Ok(())
    }
}
