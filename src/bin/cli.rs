#![allow(unused_parens)]

use std::io::Write;
use std::str::from_utf8;

use clap::{
    Parser,
    Subcommand
};
use epoch_db::db::errors::TransientError;
use tokio::io::{
    AsyncBufRead,
    AsyncBufReadExt,
    AsyncRead,
    AsyncReadExt,
    AsyncWrite,
    AsyncWriteExt,
    BufReader,
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

async fn tcp_logic(cli: Cli, mut client: Client) -> Result<String, TransientError> {
    let c = cli.command.unwrap();
    let mut buf_stream = BufStream::new(client.stream);
    match c {
        Commands::Set {
            key,
            val,
            ttl
        } => {
            let k: &[u8] = key.as_ref();
            let v: &[u8] = val.as_ref();
            client.buf.clear();
            let count = if ttl.is_some() {4} else {3};
            write!(
                client.buf,
                "*{}\r\n$3\r\nSET\r\n",
                count
            ).map_err(|e| {
                            TransientError::IOError {
                                error: e
                            }
                        })?;

            write!(client.buf, "${}\r\n", k.len()).map_err(|e| {
                            TransientError::IOError {
                                error: e
                            }
                        })?;

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

            
            if let Some(t) = ttl {
                let t_len = (t as f64).log10() as usize + 1;
                write!(client.buf, "${}\r\n", t_len).map_err(|e| {
                            TransientError::IOError {
                                error: e
                            }
                        })?;

                client.buf.extend_from_slice(&t.to_ne_bytes());
                write!(client.buf, "\r\n").map_err(|e| {
                                TransientError::IOError {
                                    error: e
                                }
                            })?;                
            }

            buf_stream
                .write_all(&client.buf)
                .await
                .map_err(|e| {
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

            // Read from the stream
            //TODO: Dont use '\n' try to ehh use other tricks
            buf_stream
                .read_until(b'\n', &mut client.buf)
                .await
                .map_err(|e| {
                    TransientError::IOError {
                        error: e
                    }
                })?;
            let res = from_utf8(&client.buf).map_err(|_| TransientError::ParsingToUTF8Error)?;
            Ok(String::from(res))
        },
        Commands::Rm {
            key
        } => {
            todo!()
        },
        Commands::Get {
            key
        } => {
            todo!()
        },
        Commands::GetMetadata {
            key
        } => {
            todo!()
        },
        Commands::IncrementFrequency {
            key
        } => {
            todo!()
        },
        Commands::Size => {
            todo!()
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
