use std::error::Error;

use epoch_db::db::errors::TransientError;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};





















#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut stream = BufReader::new(&b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"[..]);
    let mut element_size: Vec<u8> = Vec::new();
    let _ = stream.read_exact(&mut [0]).await;
    stream.read_until(b'\n', &mut element_size).await.map_err(|e| TransientError::IOError { error: e })?;
    
    println!()

    let key: Option<String> = None;
    let value: Option<String> = None;
    let ttl: Option<u64> = None;

    Ok(())
}
