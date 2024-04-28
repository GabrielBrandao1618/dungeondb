use std::{
    collections::{BTreeMap, HashMap},
    io::{self, BufReader, BufWriter, Read, Seek, Write},
    os::unix::fs::MetadataExt,
    path::PathBuf,
};

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

#[derive(Serialize, Deserialize, Clone, Default)]
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
        self.table.get(key).cloned()
    }
}
impl Iterator for Index {
    type Item = (String, DocumentSegment);

    fn next(&mut self) -> Option<Self::Item> {
        self.table.pop_first()
    }
}
#[derive(Clone)]
pub struct SSTable {
    pub index: Index,
    pub base_dir: PathBuf,
    pub file_name: String,
}

impl SSTable {
    pub fn new(
        base_dir: PathBuf,
        file_name: String,
        table: impl Iterator<Item = (String, Value)>,
    ) -> Self {
        let mut index = Index::new();

        let full_data_file_path = base_dir.join(format!("{file_name}.chest"));
        let mut w = BufWriter::new(std::fs::File::create(full_data_file_path).unwrap());

        for (key, value) in table {
            let offset = w.stream_position().unwrap();
            w.write_all(&to_vec(&value).unwrap()).unwrap();
            let length = w.stream_position().unwrap() - offset;
            index.insert(key, (offset as usize, length as usize).into());
        }
        let full_index_file_path = base_dir.join(format!("{file_name}.index"));
        std::fs::write(full_index_file_path, to_vec(&index).unwrap()).unwrap();

        Self {
            base_dir,
            index,
            file_name,
        }
    }
    pub fn from_file(base_dir: PathBuf, file_name: String) -> Self {
        Self {
            index: Index::from_file(base_dir.join(format!("{}.index", file_name))),
            base_dir,
            file_name,
        }
    }
    fn read_segment(&self, segment: DocumentSegment) -> Value {
        let data_file_path = self.base_dir.join(format!("{}.chest", self.file_name));
        let mut r = BufReader::new(std::fs::File::open(data_file_path).unwrap());
        r.seek(io::SeekFrom::Start(segment.offset as u64)).unwrap();
        let mut buff = vec![0; segment.length];
        r.read_exact(&mut buff).unwrap();
        let value: Value = from_slice(&buff).unwrap();
        value
    }
    pub fn get(&self, key: &str) -> Option<Value> {
        self.index
            .get(key)
            .map(|segment| self.read_segment(segment))
    }

    pub fn get_data_file_path(&self) -> PathBuf {
        self.base_dir.join(format!("{}.chest", self.file_name))
    }
    pub fn get_index_file_path(&self) -> PathBuf {
        self.base_dir.join(format!("{}.index", self.file_name))
    }
    pub fn _read_entire(&self) -> HashMap<String, Value> {
        let mut content = HashMap::new();
        for (key, loc) in &self.index.table {
            content.insert(key.clone(), self.read_segment(*loc));
        }
        content
    }
    pub fn delete_self(&self) {
        std::fs::remove_file(self.get_data_file_path()).unwrap();
        std::fs::remove_file(self.get_index_file_path()).unwrap();
    }
    /// This merges two sstables using the other as the priority
    pub fn merge(&mut self, other: &mut Self, new_file_name: String) -> Self {
        let self_index = std::mem::take(&mut self.index);
        let other_index = std::mem::take(&mut other.index);

        let merged = self_index
            .map(|(key, segment)| (key, self.read_segment(segment)))
            .chain(other_index.map(|(key, segment)| (key, other.read_segment(segment))));

        Self::new(self.base_dir.clone(), new_file_name, merged)
    }
    pub fn _data_file_size(&self) -> usize {
        let metadata = std::fs::metadata(self.get_data_file_path())
            .unwrap_or_else(|_| panic!("{}", self.get_data_file_path().display()));
        metadata.size() as usize
    }
}
