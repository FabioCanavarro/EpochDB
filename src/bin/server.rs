use std::error::Error;
use std::path::PathBuf;
use std::str::from_utf8;
use std::sync::Arc;
use std::time::Duration;

use epoch_db::db::errors::TransientError;
use epoch_db::DB;
use tokio::io::{
    AsyncBufReadExt,
    AsyncReadExt,
    AsyncWriteExt,
    BufReader
};
use tokio::net::{
    TcpListener,
    TcpStream
};
use tokio::spawn;

#[allow(dead_code)]
struct ParsedResponse {
    command: Command,
    key: Option<String>,
    value: Option<String>,
    ttl: Option<Duration>
}

#[allow(dead_code)]
enum Command {
    Set,
    Get,
    Rm,
    IncrementFrequency,
    GetMetadata,
    Ping,
    Size,
    Flush,
    Invalid
}

impl From<String> for Command {
    fn from(value: String) -> Self {
        match value.to_lowercase().as_str() {
            "set" => Self::Set,
            "get" => Self::Get,
            "rm" => Self::Rm,
            "increment_frequency" => Self::IncrementFrequency,
            "get_metadata" => Self::GetMetadata,
            "ping" => Self::Ping,
            "size" => Self::Size,
            "flush" => Self::Flush,
            _ => Self::Invalid
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // TODO: LAZY STATIC DB
    let addr = "localhost:3001";
    let listener = TcpListener::bind(addr).await?;
    let store = Arc::new(DB::new(&PathBuf::from("./")).unwrap()); // TODO: CHANGE THIS TO USE CLI
    loop {
        let stream = listener.accept();
        let handler = spawn(response_handler(stream.await?.0, store.clone()));
    }
}

async fn response_handler(stream: TcpStream, store: Arc<DB>) -> Result<(), TransientError> {
    let mut bufreader = BufReader::new(stream);
    let cmd = parse_command(&mut bufreader).await.unwrap();
    let feedback = execute_commands(cmd, store, &mut bufreader);

    Ok(())
}

/// Parses a single RESP command from the stream
/// This is the main entry point for the parser
async fn parse_command(
    stream: &mut BufReader<TcpStream>
) -> Result<ParsedResponse, TransientError> {
    let first_byte = stream.read_u8().await.map_err(|e| {
        TransientError::IOError {
            error: e
        }
    })?;

    // Expect a "*" for the first command
    if first_byte != b'*' {
        return Err(TransientError::InvalidCommand);
    }

    // TODO: After parsing return an error if there is more than 500 mb worth of
    // elements
    let num_elements = parse_integer(stream).await?;

    // Collect each element of the command into a vector.
    let mut command_parts = Vec::with_capacity(num_elements as usize);
    for _ in 0..num_elements {
        let part = parse_bulk_string(stream).await?;
        command_parts.push(part);
    }

    // Map the raw command parts to ParsedResponse struct
    let command_str = command_parts
        .first()
        .ok_or(TransientError::InvalidCommand)?
        .to_uppercase();
    let command = Command::from(command_str);
    let key = command_parts.get(1).cloned();
    let value = command_parts.get(2).cloned();
    let ttl = if let Some(ttl_str) = command_parts.get(3) {
        Some(Duration::from_millis(
            ttl_str
                .parse::<u64>()
                .map_err(|_| TransientError::ParsingToU64ByteFailed)?
        )) // TODO: CONFIG BUILDER LOL
    } else {
        None
    };

    Ok(ParsedResponse {
        command,
        key,
        value,
        ttl
    })
}

/// A helper function to read a line terminated by '\n' and parse it as a u64
async fn parse_integer(stream: &mut BufReader<TcpStream>) -> Result<u64, TransientError> {
    let mut buffer = Vec::new();
    stream.read_until(b'\n', &mut buffer).await.map_err(|e| {
        TransientError::IOError {
            error: e
        }
    })?;

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

/// Parses a single Bulk String from the stream (e.g., "$5\r\nhello\r\n")
async fn parse_bulk_string(stream: &mut BufReader<TcpStream>) -> Result<String, TransientError> {
    let first_byte = stream.read_u8().await.map_err(|e| {
        TransientError::IOError {
            error: e
        }
    })?;

    // Check if the first byte received is a "$"
    if first_byte != b'$' {
        return Err(TransientError::InvalidCommand);
    }

    // Parse the length of the string
    let len = parse_integer(stream).await?;

    // Read exactly `len` bytes for the data
    let mut data_buf = vec![0; len as usize];
    stream.read_exact(&mut data_buf).await.map_err(|e| {
        TransientError::IOError {
            error: e
        }
    })?;

    // The data is followed by a final "\r\n". We must consume this
    // Read 2 bytes for the CRLF
    let mut crlf_buf = [0; 2];
    stream.read_exact(&mut crlf_buf).await.map_err(|e| {
        TransientError::IOError {
            error: e
        }
    })?;
    if crlf_buf != *b"\r\n" {
        return Err(TransientError::InvalidCommand);
    }

    // Convert the data bytes to a String
    String::from_utf8(data_buf).map_err(|_| TransientError::ParsingToUTF8Error)
}

async fn execute_commands(
    parsed_reponse: ParsedResponse,
    store: Arc<DB>,
    stream: &mut BufReader<TcpStream>
) -> Result<(), TransientError> {
    let cmd = parsed_reponse.command;
    let key = parsed_reponse.key;
    let val = parsed_reponse.value;
    let ttl = parsed_reponse.ttl;
    let mut feedback: Option<Vec<u8>> = None;

    // WARNING: dont use e
    match cmd {
        Command::Set => {
            match store.set(
                &key.ok_or(TransientError::InvalidCommand)?,
                &val.ok_or(TransientError::InvalidCommand)?,
                ttl
            ) {
                Ok(_) => {
                    stream.write_all(b"+OK\r\n").await.map_err(|e| {
                        TransientError::IOError {
                            error: e
                        }
                    })?
                },
                Err(e) => {
                    stream
                        .write_all(format!("+ERR {}\r\n", e).as_bytes())
                        .await
                        .map_err(|e| {
                            TransientError::IOError {
                                error: e
                            }
                        })?
                },
            };
        },
        Command::GetMetadata => {
            match store.get_metadata(&key.ok_or(TransientError::InvalidCommand)?) {
                Ok(v) => {
                    match v {
                        Some(val) => {
                            if let Some(t) = val.ttl {
                                stream
                                    .write_all(format!("*6\r\n:{}\r\n:{}\r\n:{}\r\n", val.freq, val.created_at, t).as_bytes())
                                    .await
                                    .map_err(|e| {
                                        TransientError::IOError {
                                            error: e
                                        }
                                    })?;
                                return Ok(());

                            }
                            stream
                                .write_all(format!("*2\r\n:{}\r\n:{}\r\n", val.freq, val.created_at).as_bytes())
                                .await
                                .map_err(|e| {
                                    TransientError::IOError {
                                        error: e
                                    }
                                })?
                        },
                        None => {
                            stream.write_all(b"$-1\r\n").await.map_err(|e| {
                                TransientError::IOError {
                                    error: e
                                }
                            })?
                        },
                    }
                },
                Err(e) => {
                    stream
                        .write_all(format!("+ERR {}\r\n", e).as_bytes())
                        .await
                        .map_err(|e| {
                            TransientError::IOError {
                                error: e
                            }
                        })?
                },
            }
        },
        Command::Rm => {
            match store.remove(&key.ok_or(TransientError::InvalidCommand)?) {
                Ok(_) => {
                    stream.write_all(b"+OK\r\n").await.map_err(|e| {
                        TransientError::IOError {
                            error: e
                        }
                    })?
                },
                Err(e) => {
                    stream
                        .write_all(format!("+ERR {}\r\n", e).as_bytes())
                        .await
                        .map_err(|e| {
                            TransientError::IOError {
                                error: e
                            }
                        })?
                },
            };

            todo!()
        },
        Command::Flush => {
            match store.flush() {
                Ok(_) => {
                    stream.write_all(b"+OK\r\n").await.map_err(|e| {
                        TransientError::IOError {
                            error: e
                        }
                    })?
                },
                Err(e) => {
                    stream
                        .write_all(format!("+ERR {}\r\n", e).as_bytes())
                        .await
                        .map_err(|e| {
                            TransientError::IOError {
                                error: e
                            }
                        })?
                },
            };
        },
        Command::Get => {
            match store.get(&key.ok_or(TransientError::InvalidCommand)?) {
                Ok(v) => {
                    match v {
                        Some(val) => {
                            let size = val.len();
                            stream
                                .write_all(format!("${}\r\n{}\r\n", size, val).as_bytes())
                                .await
                                .map_err(|e| {
                                    TransientError::IOError {
                                        error: e
                                    }
                                })?
                        },
                        None => {
                            stream.write_all(b"$-1\r\n").await.map_err(|e| {
                                TransientError::IOError {
                                    error: e
                                }
                            })?
                        },
                    }
                },
                Err(e) => {
                    stream
                        .write_all(format!("+ERR {}\r\n", e).as_bytes())
                        .await
                        .map_err(|e| {
                            TransientError::IOError {
                                error: e
                            }
                        })?
                },
            }
        },
        Command::IncrementFrequency => {
            match store.increment_frequency(&key.ok_or(TransientError::InvalidCommand)?) {
                Ok(_) => {
                    stream.write_all(b"+OK\r\n").await.map_err(|e| {
                        TransientError::IOError {
                            error: e
                        }
                    })?
                },
                Err(e) => {
                    stream
                        .write_all(format!("+ERR {}\r\n", e).as_bytes())
                        .await
                        .map_err(|e| {
                            TransientError::IOError {
                                error: e
                            }
                        })?
                },
            };
        },
        Command::Ping => {
            stream.write_all(b"+PONG\r\n").await.map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?
        },
        Command::Size => {
            let size = store.get_db_size();
            stream
                .write_all(format!(":{}\r\n", size).as_bytes())
                .await
                .map_err(|e| {
                    TransientError::IOError {
                        error: e
                    }
                })?
        },
        Command::Invalid => {
            stream
                .write_all(format!("+ERR {}\r\n", TransientError::InvalidCommand).as_bytes())
                .await
                .map_err(|e| {
                    TransientError::IOError {
                        error: e
                    }
                })?
        },
    }

    Ok(())
}

/* TODO:
 *   Add a REPL during the loop, so they can issue commands such as:
 *   - Backup, Load(seperated to 2?, one for overwrite one just adds non
 *     existing keys)
 *   - Check amount of tll keys
 *   - Check highest freq
 *   - Check amount of ttl keys
 *   - Be able to Get, Set, rm directly from server for speed
 */
