use std::io::Cursor;
use std::time::Duration;

use epoch_db::server::commands::{
    Command,
    ParsedResponse
};
use epoch_db::server::parse_command;
use futures::future;
use tokio::io::BufReader;

async fn parse_test_command(input: &[u8]) -> ParsedResponse {
    let c = Cursor::new(input);
    let mut buf_reader = BufReader::new(c);
    parse_command(&mut buf_reader).await.unwrap()
}

#[tokio::test]
async fn test_parse_get_simple() {
    let input = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let r = parse_test_command(input).await;

    assert_eq!(
        r,
        ParsedResponse {
            command: Command::Get,
            key: Some(Vec::from(b"key")),
            value: None,
            ttl: None,
            len: 2
        }
    );
}

#[tokio::test]
async fn test_parse_set_with_ttl() {
    let input = b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$4\r\n1000\r\n";
    let r = parse_test_command(input).await;

    assert_eq!(
        r,
        ParsedResponse {
            command: Command::Set,
            key: Some(Vec::from(b"key")),
            value: Some(b"value".to_vec()),
            // Changed to from_millis to be explicit, since your parser uses it
            ttl: Some(Duration::from_millis(1000)),
            len: 4
        }
    );
}

#[tokio::test]
async fn test_parse_set_empty_value() {
    let input = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$0\r\n\r\n";
    let r = parse_test_command(input).await;

    assert_eq!(
        r,
        ParsedResponse {
            command: Command::Set,
            key: Some(b"key".to_vec()),
            value: Some(vec![]), // More idiomatic than [].to_vec()
            ttl: None,
            len: 3
        }
    )
}
