use std::io::ErrorKind;
use std::str::from_utf8;

use tokio::io::{
    AsyncBufReadExt,
    AsyncRead,
    AsyncReadExt,
    BufReader
};
use tracing::error;

use crate::db::errors::TransientError;
use crate::server::CLIENT_COMMAND_SIZE;


/// A helper function to read a line terminated by '\n' and parse it as a u64
pub async fn parse_integer<T: AsyncReadExt + AsyncRead + Unpin>(
    stream: &mut BufReader<T>
) -> Result<u64, TransientError> {
    let mut buffer = Vec::new();
    match stream.read_until(b'\n', &mut buffer).await {
        Ok(_) => (),
        Err(e) => {
            match e.kind() {
                ErrorKind::UnexpectedEof => return Err(TransientError::ClientDisconnected),
                _ => {
                    return Err(TransientError::IOError {
                        error: e
                    });
                }
            }
        },
    };

    // Check if the byte received contains a "\r\n" in the last 2 char
    if buffer.len() < 2 || &buffer[buffer.len() - 2..] != b"\r\n" {
        return Err(TransientError::InvalidCommand);
    }

    // Remove the "\r\n" from the command
    buffer.truncate(buffer.len() - 2);

    // Convert the bytes to a string
    let num_str = from_utf8(&buffer).map_err(|_| TransientError::ParsingToUTF8Error)?;

    // Parse the string into a number
    num_str
        .parse::<u64>()
        .map_err(|_| TransientError::InvalidCommand)
}

/// Parses a single Bulk String from the stream (e.g., "$5\r\nhello\r\n") to a
/// Vec<u8>
pub async fn parse_bulk_string<T: AsyncRead + AsyncReadExt + Unpin>(
    stream: &mut BufReader<T>
) -> Result<Vec<u8>, TransientError> {
    let first_byte = match stream.read_u8().await {
        Ok(t) => t,
        Err(e) => {
            match e.kind() {
                ErrorKind::UnexpectedEof => return Err(TransientError::ClientDisconnected),
                _ => {
                    return Err(TransientError::IOError {
                        error: e
                    });
                }
            }
        },
    };

    // Check if the first byte received is a "$"
    if first_byte != b'$' {
        return Err(TransientError::InvalidCommand);
    }
    let len = parse_integer(stream).await?;

    // TODO: Make this configurable by config builder or with option
    if len >= CLIENT_COMMAND_SIZE {
        error!(
            "Client has given a bulk string bigger than the limit: {}",
            len
        );
        return Err(TransientError::AboveSizeLimit);
    }

    // Read exactly `len` bytes for the data
    let mut data_buf = vec![0; len as usize];
    match stream.read_exact(&mut data_buf).await {
        Ok(_) => (),
        Err(e) => {
            match e.kind() {
                ErrorKind::UnexpectedEof => return Err(TransientError::ClientDisconnected),
                _ => {
                    return Err(TransientError::IOError {
                        error: e
                    });
                }
            }
        },
    };

    // The data is followed by a final "\r\n". We must consume this
    // Read 2 bytes for the CRLF
    let mut crlf_buf = [0; 2];
    match stream.read_exact(&mut crlf_buf).await {
        Ok(_) => (),
        Err(e) => {
            match e.kind() {
                ErrorKind::UnexpectedEof => return Err(TransientError::ClientDisconnected),
                _ => {
                    return Err(TransientError::IOError {
                        error: e
                    });
                }
            }
        },
    };

    if crlf_buf != *b"\r\n" {
        return Err(TransientError::InvalidCommand);
    }

    // Convert the data bytes to a String
    Ok(data_buf)
}
