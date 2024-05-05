use std::{
    collections::BTreeMap,
    io::{self, BufReader, BufWriter, Read, Seek, Write},
    iter::Peekable,
    path::PathBuf,
};

use crate::value::TimeStampedValue;
use itertools::{kmerge, Either};

use errors::{DungeonError, DungeonResult};
use rmp_serde::decode::from_read;
use rmp_serde::encode::to_vec;
use rmp_serde::from_slice;
use serde::Deserialize;
use serde::Serialize;

#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
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
        mut table: Peekable<impl Iterator<Item = (String, TimeStampedValue)>>,
    ) -> DungeonResult<Self> {
        let mut index = Index::new();

        let full_data_file_path = base_dir.join(format!("{file_name}.chest"));
        let mut w = BufWriter::new(
            std::fs::File::create(full_data_file_path)
                .map_err(|_| DungeonError::new("Could not create data file"))?,
        );
        let mut current_offset = 0;

        while let Some((key, value)) = table.next() {
            if let Some((next_key, next_val)) = table.next() {
                // Check if the next value uses the same key as the current. That can happen when
                // merging two SSTables.
                if next_key == key {
                    let length = match value.timestamp.cmp(&next_val.timestamp) {
                        std::cmp::Ordering::Less => Self::write_entry(&mut w, &next_val)?,
                        std::cmp::Ordering::Equal => Self::write_entry(&mut w, &value)?,
                        std::cmp::Ordering::Greater => Self::write_entry(&mut w, &value)?,
                    };
                    index.insert(key, (current_offset, length).into());
                    current_offset += length;
                } else {
                    let length = Self::write_entry(&mut w, &value)?;
                    index.insert(key, (current_offset, length).into());
                    current_offset += length;

                    let length = Self::write_entry(&mut w, &next_val)?;
                    index.insert(next_key, (current_offset, length).into());
                    current_offset += length;
                }
            } else {
                let length = Self::write_entry(&mut w, &value)?;
                index.insert(key, (current_offset, length).into());
                current_offset += length;
            }
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
    fn write_entry<W: Write + Seek>(w: &mut W, entry: &TimeStampedValue) -> DungeonResult<usize> {
        let parsed = to_vec(entry).map_err(|_| DungeonError::new("Could not parse value"))?;
        w.write_all(&parsed)
            .map_err(|_| DungeonError::new("Could not write to data file"))?;

        Ok(parsed.len())
    }
    pub fn from_file(base_dir: PathBuf, file_name: String) -> DungeonResult<Self> {
        Ok(Self {
            index: Index::from_file(base_dir.join(format!("{}.index", file_name)))?,
            base_dir,
            file_name,
        })
    }
    fn read_segment(&self, segment: DocumentSegment) -> DungeonResult<TimeStampedValue> {
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
        let value: TimeStampedValue =
            from_slice(&buff).map_err(|_| DungeonError::new("Could not parse value"))?;
        Ok(value)
    }
    pub fn get(&self, key: &str) -> DungeonResult<Option<TimeStampedValue>> {
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
    fn segment_reader_fn(
        &self,
    ) -> impl Fn((String, DocumentSegment)) -> DungeonResult<(String, TimeStampedValue)> + '_ {
        |(key, segment)| Ok((key, self.read_segment(segment)?))
    }
    /// Merges two sstables using the k-way merge algorithm
    pub fn merge(&mut self, other: &mut Self, new_file_name: String) -> DungeonResult<Self> {
        let self_index = std::mem::take(&mut self.index);
        let other_index = std::mem::take(&mut other.index);
        let self_values = self_index.flat_map(self.segment_reader_fn());
        let other_values = other_index.flat_map(other.segment_reader_fn());

        let merged = kmerge(vec![Either::Right(self_values), Either::Left(other_values)]);

        Self::new(self.base_dir.clone(), new_file_name, merged.peekable())
    }
}
