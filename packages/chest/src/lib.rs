mod mem_table;
mod ss_table;
mod value;

#[cfg(test)]
mod tests;

use std::{
    collections::HashMap,
    io,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use mem_table::MemTable;
use rmp_serde::{from_read, to_vec};
use serde::{Deserialize, Serialize};
use ss_table::SSTable;
use value::Value;

#[derive(Serialize, Deserialize)]
pub struct GeneralIndex {
    pub index: HashMap<String, String>,
}

impl GeneralIndex {
    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
        }
    }
    pub fn get(&self, key: &str) -> Option<String> {
        match self.index.get(key) {
            Some(found) => Some(found.clone()),
            None => None,
        }
    }
    pub fn insert(&mut self, key: String, file_name: String) {
        self.index.insert(key, file_name);
    }
    pub fn from_file(file_path: &str) -> Self {
        let file = std::fs::File::open(file_path).unwrap();
        from_read(file).unwrap()
    }
}

pub struct Chest {
    dir_path: PathBuf,
    mem_table: MemTable,
    flush_size: usize,
    general_index: GeneralIndex,
}

fn generate_sstable_name() -> String {
    let current_time = SystemTime::now();
    let elapsed = current_time.duration_since(UNIX_EPOCH).unwrap();
    format!("{}", elapsed.as_millis())
}

impl Chest {
    pub fn new(dir_path: &str, flush_size: usize) -> Self {
        let dir_path = PathBuf::from(dir_path);
        let mut general_index = GeneralIndex::new();
        if !dir_path.is_dir() {
            panic!("Expected path to be a directory");
        }
        for file in dir_path.read_dir().unwrap() {
            let ok_file = file.unwrap();
            let file_name = ok_file.file_name();
            if file_name == "chest.index" {
                general_index = GeneralIndex::from_file(ok_file.path().to_str().unwrap());
            }
        }
        Self {
            dir_path,
            mem_table: MemTable::new(),
            flush_size,
            general_index,
        }
    }
    pub fn set(&mut self, key: &str, value: Value) {
        self.mem_table.set(key, value);
        if self.mem_table.size() >= self.flush_size {
            self.flush().unwrap();
        }
    }
    pub fn get_sstable(&self, key: &str) -> Option<SSTable> {
        match self.general_index.get(key) {
            Some(found_file_name) => {
                Some(SSTable::from_file(self.dir_path.clone(), found_file_name))
            }
            None => None,
        }
    }
    pub fn get(&self, key: &str) -> Option<Value> {
        match self.mem_table.get(key) {
            None => match self.get_sstable(key) {
                Some(found_sstable) => found_sstable.get(key),
                None => None,
            },
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
        for key in ss_table.index.table.keys() {
            self.general_index
                .insert(key.to_string(), ss_table.file_name.to_string());
        }
        Ok(())
    }
    fn save_general_index(&self) -> io::Result<()> {
        let serialized = to_vec(&self.general_index).unwrap();
        let general_index_path = self.dir_path.join("chest.index");
        std::fs::write(general_index_path, serialized)?;
        Ok(())
    }
    pub fn len(&self) -> usize {
        self.mem_table.size()
    }
}

impl Drop for Chest {
    fn drop(&mut self) {
        match self.flush() {
            Ok(_) => match self.save_general_index() {
                Ok(_) => (),
                Err(_) => eprint!("Could not save general index"),
            },
            Err(_) => eprintln!("Error trying to save data to sstable"),
        }
    }
}
