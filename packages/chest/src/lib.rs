mod filter;
mod mem_table;
mod ss_table;
mod value;

#[cfg(test)]
mod tests;

use std::{
    cmp::Ordering,
    collections::BTreeSet,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use errors::{DungeonError, DungeonResult};
use filter::Filter;
use mem_table::MemTable;
use ss_table::SSTable;
use value::TimeStampedValue;

pub struct Chest {
    dir_path: PathBuf,
    mem_table: MemTable,
    flush_size: usize,
    sstables: BTreeSet<OrderedByDateSSTable>,
    max_sstable_count: usize,
    filter: Box<dyn Filter>,
}

fn generate_sstable_name() -> String {
    let current_time = SystemTime::now();
    let elapsed = current_time.duration_since(UNIX_EPOCH).unwrap();
    format!("{}", elapsed.as_nanos())
}

impl Chest {
    pub fn new(
        dir_path: &str,
        flush_size: usize,
        max_sstable_count: usize,
        mut filter: Box<dyn Filter>,
    ) -> DungeonResult<Self> {
        let mut sstables = BTreeSet::new();
        let dir_path = PathBuf::from(dir_path);
        if !dir_path.is_dir() {
            std::fs::create_dir_all(&dir_path)
                .map_err(|_| DungeonError::new("Could not create chest dir"))?;
        }
        for file in dir_path
            .read_dir()
            .map_err(|_| DungeonError::new("Could not read directory"))?
        {
            let ok_file = file.map_err(|_| DungeonError::new("Could not handle file"))?;
            let file_path = ok_file.path();
            match file_path.extension() {
                Some(ok_path) => {
                    if ok_path.to_str() == Some("index") {
                        let sstable = SSTable::from_file(
                            dir_path.clone(),
                            file_path
                                .file_stem()
                                .ok_or(DungeonError::new("Could not get file stem"))?
                                .to_str()
                                .ok_or(DungeonError::new("Could not convert file path to string"))?
                                .to_owned(),
                        )?;
                        for (key, _) in sstable.index.table.iter() {
                            filter.insert(key);
                        }
                        sstables.insert(OrderedByDateSSTable(sstable));
                    }
                }
                None => unreachable!(),
            }
        }
        Ok(Self {
            dir_path,
            mem_table: MemTable::new(),
            flush_size,
            max_sstable_count,
            sstables,
            filter,
        })
    }
    pub fn set(&mut self, key: &str, value: TimeStampedValue) -> DungeonResult<()> {
        self.mem_table.set(key, value);
        self.filter.insert(key);
        if self.mem_table.size() >= self.flush_size {
            self.flush()?;
        }
        Ok(())
    }
    pub fn get(&self, key: &str) -> DungeonResult<Option<TimeStampedValue>> {
        if !self.filter.contains(key) {
            return Ok(None);
        }
        match self.mem_table.get(key) {
            None => {
                for sstable in &self.sstables {
                    if let Some(found) = sstable.0.get(key)? {
                        return Ok(Some(found));
                    }
                }
                Ok(None)
            }
            default => Ok(default),
        }
    }
    fn flush(&mut self) -> DungeonResult<()> {
        // Maps (String, Value) into a DungeonResult<(String, Value)> so it is complatible with the
        // `new` sstable method
        let flushed = self.mem_table.flush().into_iter();
        let file_name = generate_sstable_name();
        let mut ss_table = SSTable::new(self.dir_path.clone(), file_name, flushed)?;
        if self.sstables.len() >= self.max_sstable_count {
            // Pick the oldest sstable and merge it with the new one. Since every merge result will
            // be placed at the end of the sstable list, the start will mostly have the smaller
            // ones
            let mut smaller = self
                .sstables
                .pop_first()
                .ok_or(DungeonError::new("Could not get smaller sstable"))?;
            let merged = smaller.0.merge(&mut ss_table, generate_sstable_name())?;
            self.sstables.insert(OrderedByDateSSTable(merged));
            smaller.0.delete_self()?;
            ss_table.delete_self()?;
        } else {
            self.sstables.insert(OrderedByDateSSTable(ss_table));
        }
        Ok(())
    }
    pub fn len(&self) -> usize {
        self.mem_table.size()
    }
    pub fn is_empty(&self) -> bool {
        self.len() > 0
    }
}
#[derive(Clone)]
struct OrderedByDateSSTable(SSTable);
impl OrderedByDateSSTable {
    fn get_date_milis(&self) -> u128 {
        self.0.file_name.parse().unwrap()
    }
}
impl PartialOrd for OrderedByDateSSTable {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // The more recent sstable is in the end in terms of ordering
        Some(self.get_date_milis().cmp(&other.get_date_milis()))
    }
}
impl Ord for OrderedByDateSSTable {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // The more recent sstable is in the end in terms of ordering
        self.get_date_milis().cmp(&other.get_date_milis())
    }
}
impl Eq for OrderedByDateSSTable {}
impl PartialEq for OrderedByDateSSTable {
    fn eq(&self, other: &Self) -> bool {
        self.0.file_name == other.0.file_name
    }
}

impl Drop for Chest {
    fn drop(&mut self) {
        match self.flush() {
            Ok(_) => (),
            Err(err) => eprintln!("{err}"),
        }
    }
}
