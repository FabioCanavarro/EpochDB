use std::io::Cursor;
use std::time::Duration;

use epoch_db::server::commands::{
    Command,
    ParsedResponse
};
use epoch_db::server::parse_command;
use futures::future;
use tokio::io::BufReader;

#[tokio::test]
async fn valid_command_test() {
    /*
     * *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n
     * *4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$4\r\n1000\r\n
     * *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$0\r\n\r\n
     */
    let buf_arr: &[&[u8]] = &[
        b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
        b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$4\r\n1000\r\n".as_ref(),
        b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$0\r\n\r\n"
    ];

    let res = buf_arr.iter().map(async |buf| {
        let c = Cursor::new(buf);
        let mut buf_reader = BufReader::new(c);
        parse_command(&mut buf_reader).await.unwrap()
    });
    let r = future::join_all(res).await;

    assert_eq!(
        r[0],
        ParsedResponse {
            command: Command::Get,
            key: Some(Vec::from(b"key")),
            value: None,
            ttl: None,
            len: 2
        }
    );

    assert_eq!(
        r[1],
        ParsedResponse {
            command: Command::Set,
            key: Some(Vec::from(b"key")),
            value: Some(b"value".to_vec()),
            ttl: Some(Duration::new(1, 0)),
            len: 4
        }
    );

    assert_eq!(
        r[2],
        ParsedResponse {
            command: Command::Set,
            key: Some(b"key".to_vec()),
            value: Some([].to_vec()),
            ttl: None,
            len: 3
        }
    )
}
