use itertools::Itertools;

use super::super::entry::Entry;

const FOOTER_OFFSET_LENGTH: u8 = 2;
const FOOTER_RESTART_COUNT_LENGTH: u8 = 4;

const RESTART_OFFSETS_INTERVAL: u8 = 32;

#[derive(Debug)]
pub enum BlockBuilderError {
    AddEntry,
}

pub struct BlockBuilder {
    entries: Vec<Entry>,
    restart_offsets: Vec<usize>,
    restart_count: usize,
    last_key: String,
    current_offset: usize,
}

impl BlockBuilder {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            restart_offsets: Vec::new(),
            restart_count: 0,
            last_key: String::new(),
            current_offset: 0,
        }
    }
    /// Layout: `[entries][restart offsets][restart count section]`
    pub fn encode(&self) -> String {
        let entries_data_section = self.entries.iter().map(|entry| entry.encode()).join("");
        let restart_count_section = format!(
            "{val:0length$x}",
            val = self.restart_count,
            length = FOOTER_RESTART_COUNT_LENGTH as usize
        );
        let restart_offsets_section = self
            .restart_offsets
            .iter()
            .map(|offset| {
                format!(
                    "{offset:0length$x}",
                    offset = offset,
                    length = FOOTER_OFFSET_LENGTH as usize
                )
            })
            .join("");

        format!("{entries_data_section}{restart_offsets_section}{restart_count_section}")
    }
    fn get_shared_bytes_len(&self, key: &str) -> usize {
        if self.entries.is_empty() {
            return 0;
        }
        let last_key_iter = self.last_key.chars();
        let current_key = key.chars();
        let mut shared_count = 0;

        for (last, current) in last_key_iter.zip(current_key) {
            if last == current {
                shared_count += 1;
            } else {
                break;
            }
        }
        shared_count
    }
    pub fn push(&mut self, key: String, value: String) -> Result<(), BlockBuilderError> {
        let should_restart = self.entries.len() % RESTART_OFFSETS_INTERVAL as usize == 0;
        if should_restart {
            self.restart_count += 1;
            self.last_key = key.clone();
            let new_entry = Entry::new(0, key.len(), value.len(), key, value)
                .map_err(|_| BlockBuilderError::AddEntry)?;
            self.entries.push(new_entry);
            return Ok(());
        }
        let shared_len = self.get_shared_bytes_len(&key);
        let non_shared_len = key.len() - shared_len;
        let non_shared_portion = &key[shared_len..];

        self.last_key = key.clone();
        let new_entry = Entry::new(
            shared_len,
            non_shared_len,
            value.len(),
            non_shared_portion.to_owned(),
            value,
        )
        .map_err(|_| BlockBuilderError::AddEntry)?;

        self.entries.push(new_entry);

        Ok(())
    }
}
