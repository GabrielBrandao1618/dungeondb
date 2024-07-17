use rmp_serde::{decode, encode};
use std::{error::Error, fmt::Debug};

use chest::value::Value;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ServerError {
    msg: String,
}
impl Error for ServerError {}

impl ServerError {
    pub fn new(msg: &str) -> Self {
        Self {
            msg: msg.to_owned(),
        }
    }
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum ServerResponse {
    Value(Value),
    Err(ServerError),
}

impl std::fmt::Display for ServerResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerResponse::Value(val) => match val {
                Value::Integer(v) => write!(f, "{v}"),
                Value::Float(v) => write!(f, "{v}"),
                Value::String(v) => write!(f, "{v}"),
                Value::Boolean(v) => write!(f, "{v}"),
                Value::Invalid => write!(f, "invalid"),
            },
            ServerResponse::Err(err) => std::fmt::Display::fmt(&err, f),
        }
    }
}

impl ServerResponse {
    pub fn from_value(val: Value) -> Self {
        Self::Value(val)
    }
    pub fn from_error(err: ServerError) -> Self {
        Self::Err(err)
    }

    pub fn to_vec(&self) -> Result<Vec<u8>, encode::Error> {
        encode::to_vec(self)
    }
    pub fn from_vec(src: &[u8]) -> Result<Self, decode::Error> {
        let parsed: Self = decode::from_slice(src)?;
        Ok(parsed)
    }
}
