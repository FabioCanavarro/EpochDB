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
    let cmd = parse_byte(bufreader);
    let feedback = execute_commands(cmd);

    Ok(())
}

async fn parse_byte(mut stream: BufReader<TcpStream>) -> Result<ParsedResponse, TransientError> {
    // TODO: Maximum size of usize, or they can just do *4000000000000000000
    let mut element_size: Vec<u8> = Vec::new();
    stream.read_exact(&mut [0]).await;
    stream.read_until(b'\n', &mut element_size).await.map_err(|e| TransientError::IOError { error: e })?;
    
    let key: Option<String> = None;
    let value: Option<String> = None;
    let ttl: Option<u64> = None;

    todo!()
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
