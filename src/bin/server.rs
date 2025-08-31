use std::{error::Error, net::{TcpListener, TcpStream}};

use epoch_db::DB;
use tokio::spawn;

struct ParsedResponse<'a> {
    command: String,
    key: Option<&'a str>,
    value: Option<&'a str>,
    ttl: Option<u64>
}

enum Command {
    Set,
    Get,
    Rm,
    IncrementFrequency,
    GetMetadata
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
}

async fn response_handler(stream: TcpStream) {
    todo!()
}

async fn parse_byte(byte: &[u8]) {
    todo!()
}

async fn execute_commands(db: DB, command: Command) {
    todo!()
}

async fn stream_feedback(stream: TcpStream, feedback: Result<&[u8], Box<dyn Error>>) {
    todo!()
}
