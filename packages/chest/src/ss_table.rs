use std::{
    collections::BTreeMap,
    io::{self, BufReader, BufWriter, Read, Seek, Write},
    path::PathBuf,
};

use crate::value::Value;

use errors::{DungeonError, DungeonResult};
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
    /// This method creates a SStable and index file using in the provided base_dir using the provided
    /// file_name and returns the resulting sstable
    /// Every key-value pair is a DungeonResult because the values could be comming from a file
    pub fn new(
        base_dir: PathBuf,
        file_name: String,
        table: impl Iterator<Item = DungeonResult<(String, Value)>>,
    ) -> DungeonResult<Self> {
        let mut index = Index::new();

        let full_data_file_path = base_dir.join(format!("{file_name}.chest"));
        let mut w = BufWriter::new(
            std::fs::File::create(full_data_file_path)
                .map_err(|_| DungeonError::new("Could not create data file"))?,
        );

        for item in table {
            let (key, value) = item?;
            let offset = w
                .stream_position()
                .map_err(|_| DungeonError::new("Could not get current stream position"))?;

            w.write_all(
                &to_vec(&value)
                    .map_err(|_| DungeonError::new("Could not serialize value to bytes"))?,
            )
            .map_err(|_| DungeonError::new("Could not write to data file"))?;
            let length = w
                .stream_position()
                .map_err(|_| DungeonError::new("Could not get current stream position"))?
                - offset;
            index.insert(key, (offset as usize, length as usize).into());
        }
        let full_index_file_path = base_dir.join(format!("{file_name}.index"));
        std::fs::write(
            full_index_file_path,
            to_vec(&index).map_err(|_| DungeonError::new("Could not parse data to bytes"))?,
        )
        .map_err(|_| DungeonError::new("Could not save index"))?;

        Ok(Self {
            base_dir,
            index,
            file_name,
        })
    }
    pub fn from_file(base_dir: PathBuf, file_name: String) -> DungeonResult<Self> {
        Ok(Self {
            index: Index::from_file(base_dir.join(format!("{}.index", file_name)))?,
            base_dir,
            file_name,
        })
    }
    fn read_segment(&self, segment: DocumentSegment) -> DungeonResult<Value> {
        let data_file_path = self.base_dir.join(format!("{}.chest", self.file_name));
        let mut r = BufReader::new(
            std::fs::File::open(data_file_path)
                .map_err(|_| DungeonError::new("Could not read data file"))?,
        );
        r.seek(io::SeekFrom::Start(segment.offset as u64))
            .map_err(|_| DungeonError::new("Could not access correct data location in sstable"))?;
        let mut buff = vec![0; segment.length];
        r.read_exact(&mut buff)
            .map_err(|_| DungeonError::new("Could not read data file"))?;
        let value: Value =
            from_slice(&buff).map_err(|_| DungeonError::new("Could not parse value"))?;
        Ok(value)
    }
    pub fn get(&self, key: &str) -> DungeonResult<Option<Value>> {
        let value = self
            .index
            .get(key)
            .map(|segment| self.read_segment(segment))
            .ok_or(DungeonError::new("Could not read segment"))?;
        Ok(value.ok())
    }

    pub fn get_data_file_path(&self) -> PathBuf {
        self.base_dir.join(format!("{}.chest", self.file_name))
    }
    pub fn get_index_file_path(&self) -> PathBuf {
        self.base_dir.join(format!("{}.index", self.file_name))
    }
    pub fn delete_self(&self) -> DungeonResult<()> {
        std::fs::remove_file(self.get_data_file_path())
            .map_err(|_| DungeonError::new("Could not delete data file"))?;
        std::fs::remove_file(self.get_index_file_path())
            .map_err(|_| DungeonError::new("Could not delete index file"))?;
        Ok(())
    }
    /// This merges two sstables using the other as the priority
    pub fn merge(&mut self, other: &mut Self, new_file_name: String) -> DungeonResult<Self> {
        let self_index = std::mem::take(&mut self.index);
        let other_index = std::mem::take(&mut other.index);

        let merged = self_index
            .map(|(key, segment)| -> DungeonResult<(String, Value)> {
                Ok((key, self.read_segment(segment)?))
            })
            .chain(
                other_index.map(|(key, segment)| -> DungeonResult<(String, Value)> {
                    Ok((key, other.read_segment(segment)?))
                }),
            );

        Self::new(self.base_dir.clone(), new_file_name, merged)
    }
}
