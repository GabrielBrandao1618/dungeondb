use std::{error::Error, fmt::Display};

#[derive(Debug)]
pub struct DungeonError {
    message: String,
}
impl Display for DungeonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for DungeonError {}
