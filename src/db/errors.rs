//! This module defines the custom error types used throughout the EpochDB
//! library.
use std::error::Error;
use std::fmt::Display;
use std::path::PathBuf;

/// The primary error enum for the EpochDB library.
/// Fun Fact: It's called TransientError because Transient is the old name of
/// the DB
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
        error: sled::Error
    },
    /// Error that occurs during a `sled` transaction.
    SledTransactionError,
    /// Error that occurs when parsing a byte slice to a u64 fails.
    ParsingToU64ByteFailed,
    /// Error that occurs when any folder in the path doesnt exist.
    FolderNotFound {
        path: PathBuf
    },
    /// Wrapper for `zip::result::ZipError`.
    ZipError {
        /// The underlying `zip::result::ZipError`.
        error: zip::result::ZipError
    },
    /// Error that occurs when the file doesnt exist.
    FileNameDoesntExist,
    /// Error that occurs when the corresponding Metadata doesnt exist.
    MetadataNotFound,
    /// Error that occurs when the Metadata of the database itself doesnt exist.
    DBMetadataNotFound,
    /// Error that occurs when a Mutex is poisoned.
    PoisonedMutex,
    /// Error that occurs when parsing from a byte slice to any type.
    ParsingFromByteError,
    /// Wrapper for `std::io::Error`.
    IOError {
        /// The underlying `std::io::Error`
        error: std::io::Error
    },
    InvalidCommand,
    ValueNotFound,
    ClientDisconnected,
    AboveSizeLimit,
    WrongNumberOfArguments {
        command: String,
        expected: u32,
        received: u32
    },
    ProtocolError
}

impl Display for TransientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransientError::IncretmentError => writeln!(f, "Incretment has failed"),
            TransientError::ParsingToByteError => writeln!(f, "Parsing to byte failed"),
            TransientError::ParsingToUTF8Error => writeln!(f, "Parsing to utf8 failed"),
            TransientError::SledError {
                error
            } => writeln!(f, "Sled failed {error}"),
            TransientError::SledTransactionError => writeln!(f, "Sled Transaction failed"),
            TransientError::ParsingToU64ByteFailed => {
                writeln!(f, "Failed to parse a variable to a U64 byte [u8; 8]")
            },
            TransientError::FolderNotFound {
                path
            } => {
                writeln!(f, "Folder is not found at the path: {path:#?}")
            },
            TransientError::ZipError {
                error
            } => writeln!(f, "Zip crate failed {error}"),
            TransientError::FileNameDoesntExist => writeln!(f, "File name doesnt exist"),
            TransientError::MetadataNotFound => writeln!(f, "Metadata is not found"),
            TransientError::DBMetadataNotFound => writeln!(f, "DB metadata is not found"),
            TransientError::PoisonedMutex => writeln!(f, "Mutex is poisoned"),
            TransientError::ParsingFromByteError => writeln!(f, "Parsing from byte failed"),
            TransientError::IOError {
                error
            } => writeln!(f, "std IO failed {error}"),
            TransientError::InvalidCommand => writeln!(f, "Command is invalid"),
            TransientError::ValueNotFound => writeln!(f, "Value is not found"),
            TransientError::ClientDisconnected => writeln!(f, "Client has disconnected"),
            TransientError::AboveSizeLimit => {
                writeln!(f, "Message received was above the size limit")
            },
            TransientError::WrongNumberOfArguments {
                command,
                expected,
                received
            } => {
                writeln!(
                    f,
                    "Wrong number of arguments for \"{command}\" command; Needed {expected} arguments, Received {received} arguments"
                )
            },
            TransientError::ProtocolError => {
                writeln!(f, "Invalid RESP protocol format: unexpected or malformed data received")
            }
        }
    }
}

impl Error for TransientError {}
