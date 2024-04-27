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

use mem_table::MemTable;
use ss_table::SSTable;
use value::Value;

pub struct Chest {
    dir_path: PathBuf,
    mem_table: MemTable,
    flush_size: usize,
    sstables: BTreeSet<OrderedByDateSSTable>,
    max_sstable_count: usize,
}

fn generate_sstable_name() -> String {
    let current_time = SystemTime::now();
    let elapsed = current_time.duration_since(UNIX_EPOCH).unwrap();
    format!("{}", elapsed.as_nanos())
}

impl Chest {
    pub fn new(dir_path: &str, flush_size: usize, max_sstable_count: usize) -> Self {
        let mut sstables = BTreeSet::new();
        let dir_path = PathBuf::from(dir_path);
        if !dir_path.is_dir() {
            std::fs::create_dir_all(&dir_path).expect("Could not create chest dir");
        }
        for file in dir_path.read_dir().unwrap() {
            let ok_file = file.unwrap();
            let file_path = ok_file.path();
            match file_path.extension() {
                Some(ok_path) => {
                    if ok_path.to_str() == Some("index") {
                        sstables.insert(OrderedByDateSSTable(SSTable::from_file(
                            dir_path.clone(),
                            file_path.file_stem().unwrap().to_str().unwrap().to_owned(),
                        )));
                    }
                }
                None => unreachable!(),
            }
        }
        Self {
            dir_path,
            mem_table: MemTable::new(),
            flush_size,
            max_sstable_count,
            sstables,
        }
    }
    pub fn set(&mut self, key: &str, value: Value) {
        self.mem_table.set(key, value);
        if self.mem_table.size() >= self.flush_size {
            self.flush().unwrap();
        }
    }
    pub fn get(&self, key: &str) -> Option<Value> {
        match self.mem_table.get(key) {
            None => {
                for sstable in &self.sstables {
                    if let Some(found) = sstable.0.get(key) {
                        return Some(found);
                    }
                }
                None
            }
            default => default,
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        let flushed = self.mem_table.flush();
        let file_name = generate_sstable_name();
        let ss_table = SSTable::new(
            self.dir_path.clone(),
            file_name,
            flushed.into_iter().collect(),
        );
        self.sstables.insert(OrderedByDateSSTable(ss_table));
        if self.sstables.len() > self.max_sstable_count {
            self.merge_smaller_sstables();
        }
        Ok(())
    }
    fn merge_smaller_sstables(&mut self) {
        let smaller1 = self.sstables.pop_last().unwrap();
        let smaller2 = self.sstables.pop_last().unwrap();
        let merged = smaller2.0.merge(smaller1.0, generate_sstable_name());
        self.sstables.insert(OrderedByDateSSTable(merged));
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
            Err(_) => eprintln!("Error trying to save data to sstable"),
        }
    }
}
