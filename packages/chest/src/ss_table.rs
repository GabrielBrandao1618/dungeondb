use std::collections::BTreeMap;
use std::collections::HashMap;
use std::io;
use std::io::BufReader;
use std::io::Cursor;
use std::io::Read;
use std::io::Seek;
use std::path::PathBuf;

use crate::value::Value;

use rmp_serde::decode::from_read;
use rmp_serde::encode::to_vec;
use rmp_serde::from_slice;
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
pub struct Index {
    pub table: BTreeMap<String, DocumentSegment>,
}
impl Index {
    pub fn new() -> Self {
        Self {
            table: BTreeMap::new(),
        }
    }
    pub fn from_file(file_path: PathBuf) -> Self {
        let parsed_index: Self = from_read(std::fs::File::open(file_path).unwrap()).unwrap();
        parsed_index
    }
    pub fn insert(&mut self, key: String, segment: DocumentSegment) {
        self.table.insert(key, segment);
    }
    pub fn get(&self, key: &str) -> Option<DocumentSegment> {
        match self.table.get(key) {
            Some(segment) => Some(segment.clone()),
            None => None,
        }
    }
}
pub struct SSTable {
    pub index: Index,
    pub content: Vec<u8>,
    pub base_dir: PathBuf,
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
    pub fn from_file(base_dir: PathBuf, file_name: &str) -> Self {
        let data_file_path = base_dir.join(format!("{}.chest", file_name));
        let data_file_content = std::fs::read(data_file_path).unwrap();
        Self {
            index: Index::from_file(base_dir.join(format!("{}.index", file_name))),
            content: data_file_content,
            base_dir,
        }
    }
    pub fn get(&self, key: &str) -> Option<Value> {
        match self.index.get(key) {
            Some(segment) => {
                let mut r = BufReader::new(Cursor::new(&self.content));
                r.seek(io::SeekFrom::Start(segment.offset as u64)).unwrap();
                let mut buff = vec![0; segment.length];
                r.read_exact(&mut buff).unwrap();
                let value: Value = from_slice(&buff).unwrap();
                Some(value)
            }
            _ => None,
        }
    }
}
