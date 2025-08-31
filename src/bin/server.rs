use std::{error::Error, net::{TcpListener, TcpStream}};

use epoch_db::{db::errors::TransientError, DB};
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
    let addr = "localhost:3001";
    let listener = TcpListener::bind(addr)?;
    loop {
        let stream = listener.accept()?.0;
        let handler = spawn(response_handler(stream));
    }
}

async fn response_handler(stream: TcpStream) -> Result<(), TransientError> {
    todo!()
}

async fn parse_byte(byte: &[u8]) -> Result<Command, Box<dyn Error>> {
    todo!()
}

async fn execute_commands(db: DB, command: Command) -> Result<Option<Vec<u8>>, Box<dyn Error>> {
    todo!()
}

async fn stream_feedback(stream: TcpStream, feedback: Result<&[u8], Box<dyn Error>>) -> Result<(), Box<dyn Error>> {
    todo!()
}

/* TODO:
*   Add a REPL during the loop, so they can issue commands such as:
*   - Backup, Load(seperated to 2?, one for overwrite one just adds non existing keys)
*   - Check amount of tll keys
*   - Check highest freq
*   - Check amount of ttl keys
*   - Be able to Get, Set, rm directly from server for speed
*/
