use std::{error, fmt::Display};

#[derive(Debug)]
pub struct DungeonError {
    pub message: String,
}
impl DungeonError {
    pub fn new(msg: &str) -> Self {
        Self {
            message: msg.to_owned(),
        }
    }
}
impl Display for DungeonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl error::Error for DungeonError {}
pub type DungeonResult<T> = std::result::Result<T, DungeonError>;
