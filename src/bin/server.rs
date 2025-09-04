use std::{error::Error, str::from_utf8};
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
    let cmd = parse_command(&mut bufreader).await.unwrap();
    let feedback = execute_commands(cmd);

    Ok(())
}

/// Parses a single RESP command from the stream
/// This is the main entry point for the parser
async fn parse_command(
    stream: &mut BufReader<TcpStream>,
) -> Result<ParsedResponse, TransientError> {
    let first_byte = stream.read_u8().await.map_err(|e| TransientError::IOError { error: e })?;


    // Expect a "*" for the first command 
    if first_byte != b'*' {
        return Err(TransientError::InvalidCommand);
    }

    let num_elements = parse_integer(stream).await?;

    // Collect each element of the command into a vector.
    let mut command_parts = Vec::with_capacity(num_elements as usize);
    for _ in 0..num_elements {
        let part = parse_bulk_string(stream).await?;
        command_parts.push(part);
    }
    
    // Map the raw command parts to ParsedResponse struct
     // TODO: Match the command with the command enum by making a from fn
    let command = command_parts.first().ok_or(TransientError::InvalidCommand)?.to_uppercase();
    let key = command_parts.get(1).cloned();
    let value = command_parts.get(2).cloned();
    let ttl = if let Some(ttl_str) = command_parts.get(3) {
        ttl_str.parse::<u64>().ok()
    } else {
        None
    };

    Ok(ParsedResponse {
        command,
        key,
        value,
        ttl,
    })
}

/// A helper function to read a line terminated by '\n' and parse it as a u64
async fn parse_integer(stream: &mut BufReader<TcpStream>) -> Result<u64, TransientError> {
    let mut buffer = Vec::new();
    stream.read_until(b'\n', &mut buffer).await.map_err(|e| TransientError::IOError { error: e })?;
    
    // Check if the byte received contains a "\r\n" in the last 2 char 
    if buffer.len() < 2 || &buffer[buffer.len()-2..] != b"\r\n" {
        return Err(TransientError::InvalidCommand);
    }

    // Remove the "\r\n" from the command
    buffer.truncate(buffer.len() - 2);

    // Convert the bytes to a string 
    let num_str = from_utf8(&buffer).map_err(|_| TransientError::ParsingToUTF8Error)?;

    // Parse the string into a number
    num_str.parse::<u64>().map_err(|_| TransientError::InvalidCommand)
}


/// Parses a single Bulk String from the stream (e.g., "$5\r\nhello\r\n")
async fn parse_bulk_string(
    stream: &mut BufReader<TcpStream>,
) -> Result<String, TransientError> {
    let first_byte = stream.read_u8().await.map_err(|e| TransientError::IOError { error: e })?;

    // Check if the first byte received is a "$" 
    if first_byte != b'$' {
        return Err(TransientError::InvalidCommand);
    }

    // Parse the length of the string
    let len = parse_integer(stream).await?;

    // Read exactly `len` bytes for the data
    let mut data_buf = vec![0; len as usize];
    stream.read_exact(&mut data_buf).await.map_err(|e| TransientError::IOError { error: e })?;

    // The data is followed by a final "\r\n". We must consume this
    // Read 2 bytes for the CRLF
    let mut crlf_buf = [0; 2];
    stream.read_exact(&mut crlf_buf).await.map_err(|e| TransientError::IOError { error: e })?;
    if crlf_buf != *b"\r\n" {
        return Err(TransientError::InvalidCommand);
    }
    
    // Convert the data bytes to a String
    String::from_utf8(data_buf).map_err(|_| TransientError::ParsingToUTF8Error)
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
