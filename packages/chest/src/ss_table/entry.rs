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
    ) -> Entry {
        Entry {
            shared_len,
            non_shared_len,
            value_len,
            key,
            value,
        }
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
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_encode() {
        // Lets say the previous entry was "app"
        let entry = Entry::new(2, 3, 4, "lle".to_string(), "good".to_string());

        let result = entry.encode();

        assert_eq!(result, "020304llegood");

        let entry2 = Entry::new(
            16,
            15,
            15,
            "lllllllllllllll".to_string(),
            "goooooooooooood".to_string(),
        );
        assert_eq!(entry2.encode(), "100f0flllllllllllllllgoooooooooooood");
    }
}
