use std::error::Error;
use std::io::{
    BufRead,
    BufReader,
    Read
};
use std::net::{
    TcpListener,
    TcpStream
};
use std::path::PathBuf;

use epoch_db::DB;
use epoch_db::server::ServerError;
use tokio::spawn;

#[allow(dead_code)]
struct ParsedResponse {
    command: String,
    key: Option<String>,
    value: Option<String>,
    ttl: Option<u64>
}

#[allow(dead_code)]
enum Command {
    Set,
    Get,
    Rm,
    IncrementFrequency,
    GetMetadata,
    Ping,
    DbSize,
    FlushDb
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // TODO: LAZY STATIC DB
    let addr = "localhost:3001";
    let listener = TcpListener::bind(addr)?;
    loop {
        let stream = listener.accept()?.0;
        let handler = spawn(response_handler(stream));
    }
}

async fn response_handler(stream: TcpStream) -> Result<(), ServerError> {
    let mut bufreader = BufReader::new(stream);
    let cmd = parse_byte(bufreader)?;
    let feedback = execute_commands(cmd)?;

    Ok(())
}

fn parse_byte(mut stream: BufReader<TcpStream>) -> Result<ParsedResponse, ServerError> {
    let mut element_size: Vec<u8> = Vec::new();
    stream.read_exact(&mut [0])?;
    stream.read_until(b'\n', &mut element_size)?;
    todo!()
}

fn execute_commands(command: ParsedResponse) -> Result<Option<Vec<u8>>, ServerError> {
    todo!()
}

async fn stream_feedback(
    stream: TcpStream,
    feedback: Result<&[u8], Box<dyn Error>>
) -> Result<(), Box<dyn Error>> {
    todo!()
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
