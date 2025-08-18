//! This module defines the custom error types used throughout the TransientDB library.
use std::{error::Error, fmt::Display, path::PathBuf};

/// The primary error enum for the TransientDB library.
#[derive(Debug)]
pub enum TransientError {
    /// Error that occurs during frequency increment operations.
    IncretmentError,
    /// Error that occurs when parsing to a byte slice fails.
    ParsingToByteError,
    /// Error that occurs when parsing to a UTF-8 string fails.
    ParsingToUTF8Error,
    /// Wrapper for `sled::Error`.
    SledError {
        /// The underlying `sled` error.
        error: sled::Error,
    },
    /// Error that occurs during a `sled` transaction.
    SledTransactionError,
    /// Error that occurs when parsing a byte slice to a u64 fails.
    ParsingToU64ByteFailed,
    FolderNotFound {
        path: PathBuf,
    },
    ZipError {
        error: zip::result::ZipError
    },
    FileNameDoesntExist,
    MetadataNotFound,
    DBMetadataNotFound,
    PoisonedMutex,
    ParsingFromByteError,
    IOError {
        error: std::io::Error
    }
}

impl Display for TransientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransientError::IncretmentError => writeln!(f, "Incretment has failed"),
            TransientError::ParsingToByteError => writeln!(f, "Parsing to byte failed"),
            TransientError::ParsingToUTF8Error => writeln!(f, "Parsing to utf8 failed"),
            TransientError::SledError { error } => writeln!(f, "Sled failed {error}"),
            TransientError::SledTransactionError => writeln!(f, "Sled Transaction failed"),
            TransientError::ParsingToU64ByteFailed => {
                writeln!(f, "Failed to parse a variable to a U64 byte [u8; 8]")
            },
            TransientError::FolderNotFound { path } => {
                writeln!(f, "Folder is not found at the path: {path:#?}")
            },
            TransientError::ZipError {error} => writeln!(f, "Zip crate failed {error}"),
            TransientError::FileNameDoesntExist => writeln!(f, "File name doesnt exist"),
            TransientError::MetadataNotFound => writeln!(f, "Metadata is not found"),
            TransientError::DBMetadataNotFound => writeln!(f, "DB metadata is not found"),
            TransientError::PoisonedMutex => writeln!(f, "Mutex is poisoned"),
            TransientError::ParsingFromByteError => writeln!(f, "Partsing from byte failed"),
            TransientError::IOError {error} => writeln!(f, "std IO failed {error}")
        }
    }
}

impl Error for TransientError {}
