pub mod commands;
pub mod utils;

use std::io::ErrorKind;
use std::str::from_utf8;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{
    AsyncRead,
    AsyncReadExt,
    AsyncWriteExt,
    BufReader,
    BufWriter
};
use tokio::net::tcp::WriteHalf;
use tokio::net::TcpStream;
use tracing::{
    error,
    info,
    warn
};

use crate::db::errors::TransientError;
use crate::metadata::RespValue;
use crate::server::commands::{
    Command,
    ParsedResponse
};
use crate::server::utils::{
    check_argument,
    parse_bulk_string,
    parse_integer
};
use crate::DB;

pub static CLIENT_COMMAND_SIZE: u64 = 4096;

pub async fn response_handler(mut stream: TcpStream, store: Arc<DB>) -> Result<(), TransientError> {
    let (reader, writer) = stream.split();
    let mut bufreader = BufReader::new(reader);
    let mut bufwriter = BufWriter::new(writer);

    loop {
        let cmd_err = parse_command(&mut bufreader).await;
        match cmd_err {
            Ok(cmd) => {
                match execute_commands(cmd, &store, &mut bufwriter).await {
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
                            TransientError::WrongNumberOfArguments {
                                ref command,
                                expected,
                                received
                            } => {
                                error!("Error {:?}", e);
                                bufwriter
                                    .write_all(
                                        format!("-ERR Wrong number of arguments for \"{command}\" command; Needed at least {expected} arguments, Received {received} arguments\r\n")
                                            .as_bytes()
                                    )
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
pub async fn parse_command<T: AsyncReadExt + AsyncRead + Unpin>(
    stream: &mut BufReader<T>
) -> Result<ParsedResponse, TransientError> {
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
    let command_raw = command_parts
        .first()
        .ok_or(TransientError::InvalidCommand)?;
    let command = Command::from(&command_raw[..]);
    let key = command_parts.get(1).cloned();
    let value = command_parts.get(2).cloned();

    let ttl = if let Some(ttl_raw) = command_parts.get(3) {
        Some(Duration::from_millis({
            let ttl_str = from_utf8(ttl_raw).map_err(|_| TransientError::ParsingToUTF8Error)?;
            ttl_str
                .parse::<u64>()
                .map_err(|_| TransientError::InvalidCommand)?
        })) // TODO: CONFIG BUILDER LOL
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

pub async fn execute_commands(
    parsed_reponse: ParsedResponse,
    store: &Arc<DB>,
    stream: &mut BufWriter<WriteHalf<'_>>
) -> Result<(), TransientError> {
    let cmd = parsed_reponse.command;
    let key = parsed_reponse.key;
    let val = parsed_reponse.value;
    let ttl = parsed_reponse.ttl;

    match cmd {
        Command::Set => {
            check_argument(cmd.into(), 4, parsed_reponse.len, Some(3)).await?;
            match store.set_raw(
                &&key.ok_or(TransientError::InvalidCommand)?[..],
                &&val.ok_or(TransientError::InvalidCommand)?[..],
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
            check_argument(cmd.into(), 2, parsed_reponse.len, None).await?;
            match store.get_metadata_raw(&&key.ok_or(TransientError::InvalidCommand)?[..]) {
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
            check_argument(cmd.into(), 2, parsed_reponse.len, None).await?;
            match store.remove_raw(&key.ok_or(TransientError::InvalidCommand)?[..]) {
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
            check_argument(cmd.into(), 1, parsed_reponse.len, None).await?;
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
            check_argument(cmd.into(), 2, parsed_reponse.len, None).await?;
            match store.get_raw(&&key.ok_or(TransientError::InvalidCommand)?[..]) {
                Ok(v) => {
                    match v {
                        Some(val) => {
                            let size = val.len();
                            stream
                                .write_all(format!("${}\r\n", size).as_bytes())
                                .await
                                .map_err(|e| {
                                    TransientError::IOError {
                                        error: e
                                    }
                                })?;
                            stream.write_all(&val).await.map_err(|e| {
                                TransientError::IOError {
                                    error: e
                                }
                            })?;
                            stream.write_all("\r\n".as_bytes()).await.map_err(|e| {
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
            check_argument(cmd.into(), 2, parsed_reponse.len, None).await?;
            match store.increment_frequency_raw(&key.ok_or(TransientError::InvalidCommand)?[..]) {
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
            check_argument(cmd.into(), 1, parsed_reponse.len, None).await?;
            stream.write_all(b"+PONG\r\n").await.map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?
        },
        Command::Size => {
            check_argument(cmd.into(), 1, parsed_reponse.len, None).await?;
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
