use std::error::Error;
use std::{io, usize};
use std::path::PathBuf;
use std::str::from_utf8;
use std::sync::Arc;
use std::time::Duration;

use epoch_db::db::errors::TransientError;
use epoch_db::metadata::RespValue;
use epoch_db::DB;
use tokio::io::{
    AsyncBufReadExt,
    AsyncReadExt,
    AsyncWriteExt,
    BufReader,
    BufWriter
};
use tokio::net::tcp::{
    ReadHalf,
    WriteHalf
};
use tokio::net::{
    TcpListener,
    TcpStream
};
use tokio::spawn;
use tokio::time::sleep;
use tracing::{
    error,
    info,
    warn
};
use tracing_subscriber::{
    fmt,
    EnvFilter
};

// Constants
pub const CLIENT_COMMAND_SIZE: u64 = 4096;

#[allow(dead_code)]
struct ParsedResponse {
    command: Command,
    key: Option<String>,
    value: Option<String>,
    ttl: Option<Duration>,
    len: u32
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

impl From<Command> for String {
    fn from(value: Command) -> Self {
        match value {
            Command::Set => "set".to_string(),
            Command::Rm => "rm".to_string(),
            Command::Get => "get".to_string(),
            Command::Ping => "ping".to_string(),
            Command::Size => "size".to_string(),
            Command::Flush => "flush".to_string(),
            Command::GetMetadata => "get_metadata".to_string(),
            Command::IncrementFrequency => "increment_frequency".to_string(),
            Command::Invalid => "Invalid".to_string()
        }
    }
}

async fn check_argument(stream: &mut BufWriter<WriteHalf<'_>>, command: String, expected: u32, parsed_reponse: &ParsedResponse) -> Result<(), TransientError> {
    if parsed_reponse.len > expected {
        let err = TransientError::WrongNumberOfArguments { command: command.into(), expected, received: parsed_reponse.len };
        stream
                .write_all(format!("-ERR {}\r\n", err).as_bytes())
                .await
                .map_err(|e| {
                    TransientError::IOError {
                        error: e
                    }
                })?
    }
    Ok(())

}

fn init_logger() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stdout)
        .with_ansi(true)
        .with_line_number(true)
        .with_target(true)
        .compact()
        .init();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    init_logger();

    let mut counter: i8 = 0;

    // TODO: Make this configurable
    let addr = "localhost:3001";
    let listener = TcpListener::bind(addr).await?;
    info!("Listening to {}", addr);

    // TODO: LAZY STATIC DB
    let store = Arc::new(DB::new(&PathBuf::from("./"))?); // TODO: Make path configurable

    loop {
        let stream_set = listener.accept().await;
        match stream_set {
            Ok(t) => {
                counter = 0;
                let _handler = spawn(response_handler(t.0, store.clone()));
            },
            Err(e) => {
                match e.kind() {
                    io::ErrorKind::ConnectionRefused => {
                        warn!("Connection Refused!!!");
                        continue;
                    },
                    io::ErrorKind::ConnectionAborted => {
                        warn!("Connection Aborted!!!");
                        continue;
                    },
                    _ => {
                        //TODO: test 10 times sleep 100ms, if error then break and log?
                        if counter < 10 {
                            error!("Error: {:?}", e);
                            warn!("Retry attempt: {:?}", counter);

                            counter += 1;
                            sleep(Duration::new(0, 100000000)).await;
                            continue;
                        } else {
                            error!("An Error occured: {:?}", e);
                            break;
                        }
                    }
                }
            }
        };
    }
    Ok(())
}

async fn response_handler(mut stream: TcpStream, store: Arc<DB>) -> Result<(), TransientError> {
    let (reader, writer) = stream.split();
    let mut bufreader = BufReader::new(reader);
    let mut bufwriter = BufWriter::new(writer);

    loop {
        let cmd_err = parse_command(&mut bufreader).await;
        match cmd_err {
            Ok(cmd) => {
                match execute_commands(cmd, store.clone(), &mut bufwriter).await {
                    Ok(_) => (),
                    Err(e) => {
                        match e {
                            TransientError::InvalidCommand => {
                                bufwriter
                                    .write_all(b"-ERR Wrong command issued\r\n")
                                    .await
                                    .map_err(|e| {
                                        TransientError::IOError {
                                            error: e
                                        }
                                    })?;
                            },
                            _ => {
                                error!("error: {:?}", e);
                                return Err(e);
                            }
                        }
                    },
                }
            },
            Err(e) => {
                match e {
                    TransientError::InvalidCommand => {
                        warn!("Client has issued a command with incorrect syntax");
                        bufwriter
                            .write_all(b"-ERR Wrong command issued\r\n")
                            .await
                            .map_err(|e| {
                                TransientError::IOError {
                                    error: e
                                }
                            })?;
                        bufwriter.flush().await.map_err(|e| {
                            TransientError::IOError {
                                error: e
                            }
                        })?;
                    },
                    TransientError::ClientDisconnected => {
                        info!("Client has disconnected clearly!");
                        break;
                    },
                    TransientError::AboveSizeLimit => {
                        warn!("Client has issued a command above the size limit");
                        bufwriter
                            .write_all(b"-ERR Size of command is above the limit\r\n")
                            .await
                            .map_err(|e| {
                                TransientError::IOError {
                                    error: e
                                }
                            })?;
                    },
                    _ => {
                        error!("error: {:?}", e);
                        return Err(e);
                    }
                }
            },
        }
    }
    Ok(())
}

/// Parses a single RESP command from the stream
/// This is the main entry point for the parser
async fn parse_command(
    stream: &mut BufReader<ReadHalf<'_>>
) -> Result<ParsedResponse, TransientError> {
    let first_byte = match stream.read_u8().await {
        Ok(t) => t,
        Err(e) => {
            match e.kind() {
                io::ErrorKind::UnexpectedEof => return Err(TransientError::ClientDisconnected),
                _ => {
                    return Err(TransientError::IOError {
                        error: e
                    })
                },
            }
        },
    };

    // Expect a "*" for the first command
    if first_byte != b'*' {
        return Err(TransientError::InvalidCommand);
    }

    // elements
    let num_elements = parse_integer(stream).await?;

    if num_elements > 4096 {
        warn!(
            "Client has issued more than 4096 elements: {}",
            num_elements
        );
        return Err(TransientError::AboveSizeLimit);
    }

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
        ttl,
        len: command_parts.len() as u32
    })
}

/// A helper function to read a line terminated by '\n' and parse it as a u64
async fn parse_integer(stream: &mut BufReader<ReadHalf<'_>>) -> Result<u64, TransientError> {
    let mut buffer = Vec::new();
    match stream.read_until(b'\n', &mut buffer).await {
        Ok(_) => (),
        Err(e) => {
            match e.kind() {
                io::ErrorKind::UnexpectedEof => return Err(TransientError::ClientDisconnected),
                _ => {
                    return Err(TransientError::IOError {
                        error: e
                    })
                },
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

/// Parses a single Bulk String from the stream (e.g., "$5\r\nhello\r\n")
async fn parse_bulk_string(stream: &mut BufReader<ReadHalf<'_>>) -> Result<String, TransientError> {
    let first_byte = match stream.read_u8().await {
        Ok(t) => t,
        Err(e) => {
            match e.kind() {
                io::ErrorKind::UnexpectedEof => return Err(TransientError::ClientDisconnected),
                _ => {
                    return Err(TransientError::IOError {
                        error: e
                    })
                },
            }
        },
    };

    // Check if the first byte received is a "$"
    if first_byte != b'$' {
        return Err(TransientError::InvalidCommand);
    }

    // Parse the length of the string
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
                io::ErrorKind::UnexpectedEof => return Err(TransientError::ClientDisconnected),
                _ => {
                    return Err(TransientError::IOError {
                        error: e
                    })
                },
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
                io::ErrorKind::UnexpectedEof => return Err(TransientError::ClientDisconnected),
                _ => {
                    return Err(TransientError::IOError {
                        error: e
                    })
                },
            }
        },
    };

    if crlf_buf != *b"\r\n" {
        return Err(TransientError::InvalidCommand);
    }

    // Convert the data bytes to a String
    String::from_utf8(data_buf).map_err(|_| TransientError::ParsingToUTF8Error)
}

async fn execute_commands(
    parsed_reponse: ParsedResponse,
    store: Arc<DB>,
    stream: &mut BufWriter<WriteHalf<'_>>
) -> Result<(), TransientError> {
    let cmd = parsed_reponse.command;
    let key = parsed_reponse.key;
    let val = parsed_reponse.value;
    let ttl = parsed_reponse.ttl;

    // WARNING: dont use e
    match cmd {
        Command::Set => {
            check_argument(stream, cmd.into(), 4, &parsed_reponse);
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
                        .write_all(format!("-ERR {}\r\n", e).as_bytes())
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
            check_argument(stream, cmd.into(), 2, &parsed_reponse);
            match store.get_metadata(&key.ok_or(TransientError::InvalidCommand)?) {
                Ok(v) => {
                    match v {
                        Some(val) => {
                            let array = val.to_response();
                            stream
                                .write_all(format!("*{}\r\n", array.len() * 2).as_bytes())
                                .await
                                .map_err(|e| {
                                    TransientError::IOError {
                                        error: e
                                    }
                                })?;

                            for i in array {
                                let key = i.0;
                                stream
                                    .write_all(format!("${}\r\n{}\r\n", key.len(), key).as_bytes())
                                    .await
                                    .map_err(|e| {
                                        TransientError::IOError {
                                            error: e
                                        }
                                    })?;
                                match i.1 {
                                    RespValue::U64(u) => {
                                        stream
                                            .write_all(format!(":{u}\r\n").as_bytes())
                                            .await
                                            .map_err(|e| {
                                                TransientError::IOError {
                                                    error: e
                                                }
                                            })?;
                                    },
                                    RespValue::BulkString(v) => {
                                        stream
                                            .write_all(format!("${}\r\n", v.len()).as_bytes())
                                            .await
                                            .map_err(|e| {
                                                TransientError::IOError {
                                                    error: e
                                                }
                                            })?;
                                        stream.write_all(&v).await.map_err(|e| {
                                            TransientError::IOError {
                                                error: e
                                            }
                                        })?;
                                        stream.write_all(b"\r\n").await.map_err(|e| {
                                            TransientError::IOError {
                                                error: e
                                            }
                                        })?;
                                    },
                                    RespValue::None => {
                                        stream.write_all(b"$-1\r\n").await.map_err(|e| {
                                            TransientError::IOError {
                                                error: e
                                            }
                                        })?;
                                    }
                                };
                            }
                            stream.flush().await.map_err(|e| {
                                TransientError::IOError {
                                    error: e
                                }
                            })?;
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
                        .write_all(format!("-ERR {}\r\n", e).as_bytes())
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
            check_argument(stream, cmd.into(), 2, &parsed_reponse);
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
                        .write_all(format!("-ERR {}\r\n", e).as_bytes())
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
            check_argument(stream, cmd.into(), 1, &parsed_reponse);
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
                        .write_all(format!("-ERR {}\r\n", e).as_bytes())
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
            check_argument(stream, cmd.into(), 2, &parsed_reponse);
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
                        .write_all(format!("-ERR {}\r\n", e).as_bytes())
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
            check_argument(stream, cmd.into(), 2, &parsed_reponse);
            match store.increment_frequency(&key.ok_or(TransientError::InvalidCommand)?) {
                Ok(t) => {
                    match t {
                        Some(_) => {
                            stream.write_all(b"+OK\r\n").await.map_err(|e| {
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
                        .write_all(format!("-ERR {}\r\n", e).as_bytes())
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
            check_argument(stream, cmd.into(), 1, &parsed_reponse);
            stream.write_all(b"+PONG\r\n").await.map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?
        },
        Command::Size => {
            check_argument(stream, cmd.into(), 1, &parsed_reponse);
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
                .write_all(format!("-ERR {}\r\n", TransientError::InvalidCommand).as_bytes())
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
 *   - Use Prometheus instead of the exporter
 */
