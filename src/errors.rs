use std::{error::Error, fmt::Display};

#[derive(Debug)]
pub enum DfqError {
    Unknown(String),
    InvalidArgument(String),
}

impl Display for DfqError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DfqError::Unknown(msg) => write!(f, "UnknownError: {}", msg),
            DfqError::InvalidArgument(msg) => write!(f, "InvalidArgument: {}", msg),
        }
    }
}

impl Error for DfqError {}

pub type DfqResult<T> = Result<T, DfqError>;
