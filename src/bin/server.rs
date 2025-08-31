use std::{error::Error, net::{TcpListener, TcpStream}};

use tokio::spawn;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
}

async fn response_handler(stream: TcpStream) {
    todo!()
}

async fn parse_byte(byte: &[u8]) {
    todo!()
}

async fn execute_commands(command: todo!()) {
    todo!()
}

async fn stream_feedback(stream: TcpStream, feedback: todo!()) {
    todo!()
}
