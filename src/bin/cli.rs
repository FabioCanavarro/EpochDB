#![allow(unused_parens)]
use clap::{
    Parser,
    Subcommand
};
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

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Return the helping description if they didnt specify any arguments
    if cli.command.is_none() {
        Cli::parse_from(["kvs", "--help"]);
        return;
    }

    // Bind to the address
    let mut stream = match TcpStream::connect(&cli.addr).await {
        Ok(stream) => {
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
                        },
                        None => {}
                    }
                },
                Commands::Rm {
                    key
                } => {},
                Commands::Get {
                    key
                } => {},
                Commands::GetMetadata {
                    key
                } => {},
                Commands::IncrementFrequency {
                    key
                } => {},
                Commands::Size => {},
                Commands::Flush => {},
                Commands::Ping => {}
            }
        },
        Err(e) => {
            panic!("ERROR: {}", e);
        }
    };
}
