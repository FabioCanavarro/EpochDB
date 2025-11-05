use std::{io::Cursor, sync::Arc};

use epoch_db::{DB, server::{commands::{ParsedResponse}, execute_commands, parse_command}};
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};

async fn parse_test_command(input: &[u8]) -> ParsedResponse {
    let c = Cursor::new(input);
    let mut buf_reader = BufReader::new(c);
    parse_command(&mut buf_reader).await.unwrap()
}

async fn execute_test_command(input: ParsedResponse, store: Arc<DB>) -> Vec<u8> {
    let buf: Vec<u8> = Vec::new();
    let mut c = Cursor::new(buf);
    let mut buf_writer = BufWriter::new(&mut c);
    execute_commands(input, &store, &mut buf_writer).await.unwrap();
    buf_writer.flush().await.unwrap();

    c.get_ref().to_vec()
}

#[tokio::test]
async fn test_execute_get_simple() {
    let input = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let store = Arc::new(DB::new(tempfile::tempdir().unwrap().path()).unwrap());
    store.set_raw(b"key", b"0", None).unwrap();
    
    let cmd = parse_test_command(input).await;
    let r = execute_test_command(cmd, store).await;

    assert_eq!(
        r,
        b"$1\r\n0\r\n"
    );
}


