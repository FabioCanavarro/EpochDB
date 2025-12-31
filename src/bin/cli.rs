#![allow(unused_parens, unused_variables)]
#![allow(clippy::multiple_bound_locations)]

use std::io::Write;
use std::str::from_utf8;

use async_recursion::async_recursion;
use clap::{
    Parser,
    Subcommand
};
use epoch_db::db::errors::TransientError;
use epoch_db::protocol::{
    parse_bulk_string_pure,
    parse_integer_i64,
    Response
};
use tokio::io::{
    AsyncBufReadExt,
    AsyncReadExt,
    AsyncWriteExt,
    BufStream
};
use tokio::net::TcpStream;

// Cli Parser
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    #[arg(short, long, default_value_t = ("127.0.0.1:8080".to_string()) )]
    addr: String
}

#[derive(Subcommand)]
enum Commands {
    /// Set a key-value pair
    Set {
        key: String,
        val: String,
        ttl: Option<u64>
    },

    /// Get the value for a key
    Get { key: String },

    /// Remove a key-value pair
    Rm { key: String },

    /// Increase the frequency of a key
    IncrementFrequency { key: String },

    /// Get the metadata for a key
    GetMetadata { key: String },

    /// Ping a IP address
    Ping,

    /// Get the size of the database
    Size,

    /// Flush the database
    Flush
}

#[derive(Debug)]
struct Client {
    stream: TcpStream,
    buf: Vec<u8>
}

#[async_recursion]
async fn parse_server_response<T: AsyncReadExt + Unpin + AsyncBufReadExt + Send>(
    stream: &mut T,
    buf: &mut Vec<u8>
) -> Result<Response, TransientError> {
    let first = stream.read_u8().await.map_err(|e| {
        TransientError::IOError {
            error: e
        }
    })?;
    buf.clear();

    match first {
        b'+' => {
            let res = stream.read_until(b'\n', buf).await.map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?;
            Ok(Response::SimpleString(
                from_utf8(buf)
                    .map_err(|_| TransientError::ParsingToUTF8Error)?
                    .trim_end()
                    .to_string()
            ))
        },
        b'-' => {
            let res = stream.read_until(b'\n', buf).await.map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?;

            Ok(Response::Error(
                from_utf8(buf)
                    .map_err(|_| TransientError::ParsingToUTF8Error)?
                    .trim_end()
                    .to_string()
            ))
        },
        b':' => {
            let res = parse_integer_i64(stream);
            Ok(Response::Integer(res.await?))
        },
        b'$' => {
            let l = parse_integer_i64(stream).await?;
            if l == -1 {
                Ok(Response::Null)
            } else {
                let res = parse_bulk_string_pure(stream, l);
                Ok(Response::BulkString(res.await?))
            }
        },
        b'*' => {
            let mut res_v: Vec<Response> = Vec::new();

            let l = parse_integer_i64(stream).await?;

            for i in 0..l {
                let val = parse_server_response(stream, buf);
                res_v.push(val.await?)
            }

            Ok(Response::Array(res_v))
        },
        _ => Err(TransientError::ProtocolError)?
    }
}

async fn tcp_logic(cli: Cli, mut client: Client) -> Result<String, TransientError> {
    let c = cli.command.unwrap();
    let mut buf_stream = BufStream::new(client.stream);
    match c {
        Commands::Set {
            key,
            val,
            ttl
        } => {
            // Initiate a the key and value as ref, so i do not need to keep calling
            // .as_ref()
            let k: &[u8] = key.as_ref();
            let v: &[u8] = val.as_ref();

            // Clear the buffer, to use the buffer
            client.buf.clear();

            // Get the number of elements that will be sent
            let count = if ttl.is_some() { 4 } else { 3 };

            // Write the initial header, the number of elements and the command
            write!(client.buf, "*{}\r\n$3\r\nSET\r\n", count).map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?;

            // Writing the length of the next element
            write!(client.buf, "${}\r\n", k.len()).map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?;

            // Extending the client.buf instead of using write! to ensure binary safety
            client.buf.extend_from_slice(k);
            write!(client.buf, "\r\n").map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?;

            write!(client.buf, "${}\r\n", v.len()).map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?;

            client.buf.extend_from_slice(v);
            write!(client.buf, "\r\n").map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?;

            // checks if the ttl is none
            if let Some(t) = ttl {
                // get the lenght of the ttl, by getting the number of digits in the ttl, by
                // simply taking the log10 of the ttl and ignoring the decimals
                // 120; log10(120) = 2.xxxx => 2 => 2+1 == 3, 120 has 3 digit
                // It also handles where t = 0, which may crash shit
                let t_len = if t > 0 {
                    (t as f64).log10() as usize + 1
                } else {
                    1
                };
                write!(client.buf, "${}\r\n", t_len).map_err(|e| {
                    TransientError::IOError {
                        error: e
                    }
                })?;

                write!(client.buf, "{}\r\n", t).map_err(|e| {
                    TransientError::IOError {
                        error: e
                    }
                })?;
            }

            // Write the buffer into the stream
            buf_stream.write_all(&client.buf).await.map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?;

            // Flush the stream to make sure that, data fully gets through the stream
            buf_stream.flush().await.map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?;
            // Clearing the buffer, to be able to allocate the data
            client.buf.clear();

            parse_server_response(&mut buf_stream, &mut client.buf).await?;

            // Convert the response into a string
            let res = from_utf8(&client.buf).map_err(|_| TransientError::ParsingToUTF8Error)?;

            Ok(String::from(res))
        },
        Commands::Rm {
            key
        } => {
            // Initiate a the key and value as ref, so i do not need to keep calling
            // .as_ref()
            let k: &[u8] = key.as_ref();

            // Clear the buffer, to use the buffer
            client.buf.clear();

            // Write the initial header, the number of elements and the command
            write!(client.buf, "*{}\r\n$2\r\nRM\r\n", 2).map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?;

            // Writing the length of the next element
            write!(client.buf, "${}\r\n", k.len()).map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?;

            // Extending the client.buf instead of using write! to ensure binary safety
            client.buf.extend_from_slice(k);
            write!(client.buf, "\r\n").map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?;

            // Write the buffer into the stream
            buf_stream.write_all(&client.buf).await.map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?;

            // Flush the stream to make sure that, data fully gets through the stream
            buf_stream.flush().await.map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?;

            // Clearing the buffer, to be able to allocate the data
            client.buf.clear();

            parse_server_response(&mut buf_stream, &mut client.buf).await?;

            // Convert the response into a string
            let res = from_utf8(&client.buf).map_err(|_| TransientError::ParsingToUTF8Error)?;

            Ok(String::from(res))
        },
        Commands::Get {
            key
        }
        | Commands::IncrementFrequency {
            key
        } => {
            // Initiate a the key and value as ref, so i do not need to keep calling
            // .as_ref()
            let k: &[u8] = key.as_ref();

            // Clear the buffer, to use the buffer
            client.buf.clear();

            match c {
                Commands::Get {
                    key
                } => {
                    // Write the initial header, the number of elements and the command
                    write!(client.buf, "*{}\r\n$3\r\nGET\r\n", 2).map_err(|e| {
                        TransientError::IOError {
                            error: e
                        }
                    })?;
                },
                Commands::IncrementFrequency {
                    key
                } => {
                    // Write the initial header, the number of elements and the command
                    write!(client.buf, "*{}\r\n$19\r\nINCREMENT_FREQUENCY\r\n", 2).map_err(
                        |e| {
                            TransientError::IOError {
                                error: e
                            }
                        }
                    )?;
                },
                _ => Err(TransientError::ProtocolError)?
            }

            // Writing the length of the next element
            write!(client.buf, "${}\r\n", k.len()).map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?;

            // Extending the client.buf instead of using write! to ensure binary safety
            client.buf.extend_from_slice(k);
            write!(client.buf, "\r\n").map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?;

            // Write the buffer into the stream
            buf_stream.write_all(&client.buf).await.map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?;

            // Flush the stream to make sure that, data fully gets through the stream
            buf_stream.flush().await.map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?;

            // Clearing the buffer, to be able to allocate the data
            client.buf.clear();

            parse_server_response(&mut buf_stream, &mut client.buf).await?;

            // Convert the response into a string
            let res = from_utf8(&client.buf).map_err(|_| TransientError::ParsingToUTF8Error)?;

            Ok(String::from(res))
        },
        Commands::GetMetadata {
            key
        } => {
            todo!()
        },
        Commands::Size => {
            // Clear the buffer, to use the buffer
            client.buf.clear();

            // Write the initial header, the number of elements and the command
            write!(client.buf, "*{}\r\n$4\r\nSIZE\r\n", 2).map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?;

            // Write the buffer into the stream
            buf_stream.write_all(&client.buf).await.map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?;

            // Flush the stream to make sure that, data fully gets through the stream
            buf_stream.flush().await.map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?;

            // Clearing the buffer, to be able to allocate the data
            client.buf.clear();

            parse_server_response(&mut buf_stream, &mut client.buf).await?;

            // Convert the response into a string
            let res = from_utf8(&client.buf).map_err(|_| TransientError::ParsingToUTF8Error)?;

            Ok(String::from(res))
        },
        Commands::Flush => {
            todo!()
        },
        Commands::Ping => {
            todo!()
        }
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Return the helping description if they didnt specify any arguments
    if cli.command.is_none() {
        Cli::parse_from(["kvs", "--help"]);
        return;
    }

    // Bind to the address
    let stream = match TcpStream::connect(&cli.addr).await {
        Ok(stream) => {
            let client = Client {
                stream,
                buf: Vec::new()
            };
            tcp_logic(cli, client).await
        },
        Err(e) => {
            Err(e).map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })
        },
    };

    // Matches the error from stream
    match stream {
        Ok(_) => {
            todo!()
        },
        Err(_) => {
            todo!()
        }
    }
}
