use std::{error, fmt::Display};

#[derive(Debug)]
pub struct Error {
    pub message: String,
}
impl Error {
    pub fn new(msg: &str) -> Self {
        Self {
            message: msg.to_owned(),
        }
    }
}
impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;
