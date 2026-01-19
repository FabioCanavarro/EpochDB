#![allow(unused_parens, unused_variables)]
#![allow(clippy::multiple_bound_locations)]

use std::io::Write;
use std::str::from_utf8;

use async_recursion::async_recursion;
use clap::{
    Parser,
    Subcommand
};
use colored::Colorize;
use epoch_db::db::errors::TransientError;
use epoch_db::protocol::{
    Response,
    parse_bulk_string_pure,
    parse_integer_i64
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
async fn handle_response(res: Result<Response, TransientError>) -> Result<(), TransientError> {
    match res {
        Ok(r) => {
            match r {
                Response::SimpleString(rs) => {
                    println!("{}", rs.green())
                },
                Response::Integer(i) => {
                    println!("{}", i)
                },
                // MAKE THIS RECURSIVE YAYYYY
                Response::Array(a) => {
                    let mut c = 1;
                    for i in a {
                        print!("{c}) ");
                        handle_response(Ok(i)).await?;
                        c += 1
                    }
                },
                Response::BulkString(bs) => {
                    let s = from_utf8(&bs);
                    match s {
                        Ok(ss) => println!("{}", ss),
                        Err(e) => {
                            println!("{:?}", e) // IDK WHAT THE FUCK I SHOULD DO
                            // LMAO
                        }
                    }
                },
                Response::Error(e) => {
                    println!("{} {}", "ERROR: ".red(), e.red())
                },
                Response::Null => {
                    println!("nil")
                }
            }
        },
        Err(e) => {
            println!("I am probably dumb as fuck, error: {:#?}", e)
        }
    }
    Ok(())
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

async fn tcp_logic(cli: Cli, mut client: Client) -> Result<Response, TransientError> {
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
            write!(client.buf, "*2\r\n$2\r\nRM\r\n").map_err(|e| {
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
        },
        Commands::Get {
            key
        } => {
            // Initiate a the key and value as ref, so i do not need to keep calling
            // .as_ref()
            let k: &[u8] = key.as_ref();

            // Clear the buffer, to use the buffer
            client.buf.clear();

            // Write the initial header, the number of elements and the command
            write!(client.buf, "*2\r\n$3\r\nGET\r\n").map_err(|e| {
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
        },
        Commands::IncrementFrequency {
            key
        } => {
            // Initiate a the key and value as ref, so i do not need to keep calling
            // .as_ref()
            let k: &[u8] = key.as_ref();

            // Clear the buffer, to use the buffer
            client.buf.clear();

            // Write the initial header, the number of elements and the command
            write!(client.buf, "*2\r\n$19\r\nINCREMENT_FREQUENCY\r\n").map_err(|e| {
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
        },
        Commands::GetMetadata {
            key
        } => {
            // Initiate a the key and value as ref, so i do not need to keep calling
            // .as_ref()
            let k: &[u8] = key.as_ref();

            // Clear the buffer, to use the buffer
            client.buf.clear();

            // Write the initial header, the number of elements and the command
            write!(client.buf, "*2\r\n$12\r\nGET_METADATA\r\n").map_err(|e| {
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
        },
        Commands::Size => {
            // Clear the buffer, to use the buffer
            client.buf.clear();

            // Write the initial header, the number of elements and the command
            write!(client.buf, "*1\r\n$4\r\nSIZE\r\n").map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?;
        },
        Commands::Flush => {
            // Clear the buffer, to use the buffer
            client.buf.clear();

            // Write the initial header, the number of elements and the command
            write!(client.buf, "*1\r\n$5\r\nFLUSH\r\n").map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?;
        },
        Commands::Ping => {
            // Clear the buffer, to use the buffer
            client.buf.clear();

            // Write the initial header, the number of elements and the command
            write!(client.buf, "*1\r\n$4\r\nPING\r\n").map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })?;
        }
    }

    // NOTE: Writing, flushing and reading response
    //
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

    // Flush the stream to make sure that, data fully gets through the stream
    // Clearing the buffer, to be able to allocate the data
    client.buf.clear();

    let res = parse_server_response(&mut buf_stream, &mut client.buf).await?;

    Ok(res)
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Return the helping description if they didnt specify any arguments
    if cli.command.is_none() {
        Cli::parse_from(["kvs", "--help"]);
        return;
    }

    // WARN: Single connection only, wastes a lot, we can have a repl later?
    //
    // Bind to the address
    let res = match TcpStream::connect(&cli.addr).await {
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

    if let Err(e) = handle_response(res).await {
        println!("Error: {}", e)
    }
}
