use std::{error::Error, num::ParseIntError, str::Chars};

const ENTRY_ENCODING_NUMERICAL_BASE: usize = 16;
const SHARED_LEN_SECTION_LENGTH: usize = 2;
const NON_SHARED_LEN_SECTION_LENGTH: usize = 2;
const VALUE_LEN_SECTION_LENGTH: usize = 2;

const SHARED_LEN_SECTION_MAX_SIZE: usize =
    ENTRY_ENCODING_NUMERICAL_BASE.pow(SHARED_LEN_SECTION_LENGTH as u32) - 1;
const NON_SHARED_LEN_SECTION_MAX_SIZE: usize =
    ENTRY_ENCODING_NUMERICAL_BASE.pow(NON_SHARED_LEN_SECTION_LENGTH as u32) - 1;
const VALUE_LEN_SECTION_MAX_SIZE: usize =
    ENTRY_ENCODING_NUMERICAL_BASE.pow(VALUE_LEN_SECTION_LENGTH as u32) - 1;

#[derive(Debug)]
enum EntryError {
    ErrDecodeEntry,
    ErrCreateEntry,
}

#[derive(Eq, PartialEq, PartialOrd, Ord, Debug)]
pub struct Entry {
    shared_len: usize,
    non_shared_len: usize,
    value_len: usize,
    key: String,
    value: String,
}

impl Entry {
    pub fn new(
        shared_len: usize,
        non_shared_len: usize,
        value_len: usize,
        key: String,
        value: String,
    ) -> Result<Entry, EntryError> {
        if shared_len > SHARED_LEN_SECTION_MAX_SIZE {
            return Err(EntryError::ErrCreateEntry);
        }
        if non_shared_len > NON_SHARED_LEN_SECTION_MAX_SIZE {
            return Err(EntryError::ErrCreateEntry);
        }
        if value_len > VALUE_LEN_SECTION_MAX_SIZE {
            return Err(EntryError::ErrCreateEntry);
        }
        Ok(Entry {
            shared_len,
            non_shared_len,
            value_len,
            key,
            value,
        })
    }
    /// Entry layoyt: `[shared_len:2][non_shared_len:2][value_len:2][key][value]`
    /// Each :i is the amount of bytes the section takes
    pub fn encode(&self) -> String {
        format!(
            "{shared_len:02x}{non_shared_len:02x}{value_len:02x}{key}{value}",
            key = self.key,
            value = self.value,
            shared_len = self.shared_len,
            non_shared_len = self.non_shared_len,
            value_len = self.value_len,
        )
    }
    pub fn decode(mut content: Chars) -> Result<Self, EntryError> {
        let shared_len_bytes: String = content.by_ref().take(2).collect();
        let shared_len =
            usize::from_str_radix(&shared_len_bytes, 16).map_err(|_| EntryError::ErrDecodeEntry)?;

        let non_shared_len_bytes: String = content.by_ref().take(2).collect();
        let non_shared_len = usize::from_str_radix(&non_shared_len_bytes, 16)
            .map_err(|_| EntryError::ErrDecodeEntry)?;

        let value_len_bytes: String = content.by_ref().take(2).collect();
        let value_len =
            usize::from_str_radix(&value_len_bytes, 16).map_err(|_| EntryError::ErrDecodeEntry)?;

        let key: String = content.by_ref().take(non_shared_len).collect();
        let value: String = content.by_ref().take(value_len).collect();

        Ok(Self {
            shared_len,
            non_shared_len,
            value_len,
            key,
            value,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode() {
        // Lets say the previous entry was "app"
        let entry = Entry::new(2, 3, 4, "lle".to_string(), "good".to_string()).unwrap();

        let result = entry.encode();

        assert_eq!(result, "020304llegood");

        let entry2 = Entry::new(
            16,
            15,
            15,
            "lllllllllllllll".to_string(),
            "goooooooooooood".to_string(),
        )
        .unwrap();
        assert_eq!(entry2.encode(), "100f0flllllllllllllllgoooooooooooood");
    }
    #[test]
    fn test_decode() {
        let entry = Entry::new(2, 3, 4, "lle".to_string(), "good".to_string()).unwrap();

        let encoded = entry.encode();
        let decoded = Entry::decode(encoded.chars()).unwrap();

        assert_eq!(entry, decoded)
    }

    #[test]
    fn test_create_with_overflow() {
        let ok_entry1 = Entry::new(255, 3, 4, "lle".to_string(), "good".to_string());
        assert!(ok_entry1.is_ok());
        let entry1 = Entry::new(256, 3, 4, "lle".to_string(), "good".to_string());
        assert!(entry1.is_err());

        let ok_entry2 = Entry::new(2, 255, 4, "x".repeat(255), "good".to_string());
        assert!(ok_entry2.is_ok());
        let entry2 = Entry::new(2, 256, 4, "x".repeat(256), "good".to_string());
        assert!(entry2.is_err());

        let ok_entry3 = Entry::new(2, 3, 255, "lle".to_string(), "x".repeat(255));
        assert!(ok_entry3.is_ok());
        let entry3 = Entry::new(2, 3, 256, "lle".to_string(), "x".repeat(256));
        assert!(entry3.is_err());
    }
}
