use std::fmt::Display;
use std::error::Error;

pub mod error;

#[derive(Debug)]
pub enum ServerError {
    ParsingFailed
}

impl Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::ParsingFailed => writeln!(f, "Parsing has failed!")
        }
    }
}

impl Error for ServerError {}
