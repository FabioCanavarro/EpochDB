use std::error::Error;
use tokio::{io::{AsyncBufReadExt, AsyncReadExt, BufReader}, net::{
    TcpListener,
    TcpStream
}};
use epoch_db::{db::errors::TransientError};
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
    let listener = TcpListener::bind(addr).await?;
    loop {
        let stream = listener.accept();
        let handler = spawn(response_handler(stream.await?.0));
    }
}

async fn response_handler(stream: TcpStream) -> Result<(), TransientError> {
    let mut bufreader = BufReader::new(stream);
    let cmd = parse_byte(bufreader).await.unwrap();
    let feedback = execute_commands(cmd);

    Ok(())
}

async fn parse_byte(mut stream: BufReader<TcpStream>) -> Result<ParsedResponse, TransientError> {
    // TODO: Maximum size of usize, or they can just do *4000000000000000000
    let mut element_size: Vec<u8> = Vec::new();
    let _ = stream.read_exact(&mut [0]).await;
    stream.read_until(b'\n', &mut element_size).await.map_err(|e| TransientError::IOError { error: e })?;

    let key: Option<String> = None;
    let value: Option<String> = None;
    let ttl: Option<u64> = None;

    todo!()
}

/// A helper function to read a line terminated by '\n' and parse it as a u64.
async fn parse_integer(stream: &mut BufReader<TcpStream>) -> Result<u64, TransientError> {
    let mut buffer = Vec::new();
    stream.read_until(b'\n', &mut buffer).await.map_err(|e| TransientError::IOError { error: e })?;
    
    // The buffer now contains the number and a trailing "\r\n".
    // We need to trim the last two characters.
    if buffer.len() < 2 || &buffer[buffer.len()-2..] != b"\r\n" {
        return Err(TransientError::InvalidCommand);
    }
    buffer.truncate(buffer.len() - 2);

    // Convert the bytes to a string, then parse the string into a number.
    let num_str = from_utf8(&buffer).map_err(|_| TransientError::ParsingToUTF8Error)?;
    num_str.parse::<u64>().map_err(|_| TransientError::InvalidCommand)
}

fn execute_commands(command: ParsedResponse) -> Result<Option<Vec<u8>>, TransientError> {
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
