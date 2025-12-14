#![allow(unused_parens)]
use std::error::Error;

use clap::{
    Parser,
    Subcommand
};
use epoch_db::db::errors::TransientError;
use tokio::io::AsyncWriteExt;
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

async fn tcp_logic(cli: Cli, mut stream: TcpStream) -> Result<(), TransientError> {
    let c = cli.command.unwrap();
    match c {
        Commands::Set {
            key,
            val,
            ttl
        } => {
            match ttl {
                Some(t) => {
                    let ts = t.to_string();
                    let d = format!(
                        "*4\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                        key.len(),
                        key,
                        val.len(),
                        val,
                        ts.len(),
                        ts
                    );
                    stream.write_all(d.as_bytes())
                        .await
                        .map_err(|e| TransientError::IOError { error: e })
                },
                None => {todo!()}
            }
        },
        Commands::Rm {
            key
        } => {todo!()},
        Commands::Get {
            key
        } => {todo!()},
        Commands::GetMetadata {
            key
        } => {todo!()},
        Commands::IncrementFrequency {
            key
        } => {todo!()},
        Commands::Size => {todo!()},
        Commands::Flush => {todo!()},
        Commands::Ping => {todo!()}
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
            tcp_logic(cli, stream).await
        },
        Err(e) => {
            Err(e).map_err(|e| TransientError::IOError { error: e })
        }
    };

    // Matches the error from stream
    match stream {
        Ok(_) => {
            todo!()
        }
        Err(_) => {
            todo!()
        }
    }
}
























