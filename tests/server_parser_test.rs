use std::io::Cursor;
use std::time::Duration;

use epoch_db::db::errors::TransientError;
use epoch_db::server::commands::{
    Command,
    ParsedResponse
};
use epoch_db::server::parse_command;
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

#[tokio::test]
async fn test_parse_set_binary_value() {
    let input = b"*3\r\n$3\r\nSET\r\n$4\r\n\xDE\xAD\xBE\xEF\r\n$2\r\n\xCA\xFE\r\n";
    let r = parse_test_command(input).await;

    assert_eq!(
        r,
        ParsedResponse {
            command: Command::Set,
            key: Some(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            value: Some(vec![0xCA, 0xFE]),
            ttl: None,
            len: 3
        }
    )
}

#[tokio::test]
async fn test_parse_remove_simple() {
    let input = b"*2\r\n$2\r\nRM\r\n$3\r\nkey\r\n";
    let r = parse_test_command(input).await;

    assert_eq!(
        r,
        ParsedResponse {
            command: Command::Rm,
            key: Some(b"key".to_vec()),
            value: None,
            ttl: None,
            len: 2
        }
    )
}

#[tokio::test]
async fn test_parse_increment_frequency_simple() {
    let input = b"*2\r\n$19\r\nINCREMENT_FREQUENCY\r\n$8\r\ncounter1\r\n";
    let r = parse_test_command(input).await;

    assert_eq!(
        r,
        ParsedResponse {
            command: Command::IncrementFrequency,
            key: Some(b"counter1".to_vec()),
            value: None,
            ttl: None,
            len: 2
        }
    )
}

#[tokio::test]
async fn test_parse_get_metadata_simple() {
    let input = b"*2\r\n$12\r\nGET_METADATA\r\n$3\r\nkey\r\n";
    let r = parse_test_command(input).await;

    assert_eq!(
        r,
        ParsedResponse {
            command: Command::GetMetadata,
            key: Some(b"key".to_vec()),
            value: None,
            ttl: None,
            len: 2
        }
    )
}

#[tokio::test]
async fn test_parse_size() {
    let input = b"*1\r\n$4\r\nSIZE\r\n";
    let r = parse_test_command(input).await;

    assert_eq!(
        r,
        ParsedResponse {
            command: Command::Size,
            key: None,
            value: None,
            ttl: None,
            len: 1
        }
    )
}

#[tokio::test]
async fn test_parse_ping() {
    let input = b"*1\r\n$4\r\nPING\r\n";
    let r = parse_test_command(input).await;

    assert_eq!(
        r,
        ParsedResponse {
            command: Command::Ping,
            key: None,
            value: None,
            ttl: None,
            len: 1
        }
    )
}

#[tokio::test]
async fn test_parse_flush() {
    let input = b"*1\r\n$5\r\nFLUSH\r\n";
    let r = parse_test_command(input).await;

    assert_eq!(
        r,
        ParsedResponse {
            command: Command::Flush,
            key: None,
            value: None,
            ttl: None,
            len: 1
        }
    )
}

#[tokio::test]
async fn test_parse_command_case_sensitivity() {
    let input = b"*2\r\n$3\r\nget\r\n$3\r\nkey\r\n";
    let r = parse_test_command(input).await;

    assert_eq!(
        r,
        ParsedResponse {
            command: Command::Get,
            key: Some(b"key".to_vec()),
            value: None,
            ttl: None,
            len: 2
        }
    )
}

#[tokio::test]
async fn test_parse_error_element_size_above_size_limit() {
    let input = b"*10000\r\n$5\r\nFLUSH\r\n";
    let c = Cursor::new(input);
    let mut buf_reader = BufReader::new(c);
    let r = parse_command(&mut buf_reader).await;

    match r {
        Ok(_) => panic!("Parser accepted input with 10000 elements, which is above size limit"),
        Err(e) => {
            match e {
                TransientError::AboveSizeLimit => (),
                _ => panic!("{e}")
            }
        },
    }
}

#[tokio::test]
async fn test_parse_error_bulk_string_size_above_size_limit() {
    let input = b"*2\r\n$3\r\nGET\r\n$10000000\r\nkey\r\n";
    let c = Cursor::new(input);
    let mut buf_reader = BufReader::new(c);
    let r = parse_command(&mut buf_reader).await;

    match r {
        Ok(_) => panic!("Parser accepted input with 10000 elements, which is above size limit"),
        Err(e) => {
            match e {
                TransientError::AboveSizeLimit => (),
                _ => panic!("{e}")
            }
        },
    }
}
