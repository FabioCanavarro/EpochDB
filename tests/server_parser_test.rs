use std::io::Cursor;

use epoch_db::server::commands::ParsedResponse;
use epoch_db::server::parse_command;
use tokio::io::BufReader;

#[tokio::test]
async fn valid_command_test() {
    /*
     * *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n
     * *4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$4\r\n1000\r\n
     * *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$0\r\n\r\n
     */
    let buf = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let c = Cursor::new(buf);

    let mut buf_reader = BufReader::new(c);

    assert_eq!(
        parse_command(&mut buf_reader).await.unwrap(),
        ParsedResponse {
            command: epoch_db::server::commands::Command::Get,
            key: Some(Vec::from(b"key")),
            value: None,
            ttl: None,
            len: 2
        }
    )
}
