mod mem_table;
mod ss_table;
mod value;

#[cfg(test)]
mod tests;

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use mem_table::MemTable;
use ss_table::SSTable;
use value::Value;

pub struct Chest {
    dir_path: PathBuf,
    mem_table: MemTable,
    flush_size: usize,
    general_index: HashMap<String, String>,
}

impl Chest {
    pub fn new<P: AsRef<Path>>(dir_path: P, flush_size: usize) -> Self {
        Self {
            dir_path: PathBuf::new().join(dir_path),
            mem_table: MemTable::new(),
            flush_size,
            general_index: HashMap::new(),
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
            None => match self.general_index.get(key) {
                Some(found_file_name) => {
                    let sstable = SSTable::from_file(self.dir_path.clone(), &found_file_name);
                    sstable.get(key)
                }
                None => None,
            },
            default => default,
        }
    }
    pub fn flush(&mut self) -> std::io::Result<()> {
        let flushed = self.mem_table.flush();
        let ss_table = SSTable::new(self.dir_path.clone(), flushed.into_iter().collect());
        let file_name = cuid::cuid2();
        ss_table.write(&file_name)?;
        for (key, _) in ss_table.index.table {
            self.general_index.insert(key, file_name.to_string());
        }
        Ok(())
    }
    pub fn len(&self) -> usize {
        self.mem_table.size()
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
