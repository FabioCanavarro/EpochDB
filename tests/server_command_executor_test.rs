use std::io::Cursor;
use std::str::from_utf8;
use std::sync::Arc;
use std::time::Duration;

use epoch_db::server::commands::ParsedResponse;
use epoch_db::server::{
    execute_commands,
    parse_command
};
use epoch_db::DB;
use tokio::io::{
    AsyncWriteExt,
    BufReader,
    BufWriter
};
use tokio::time::sleep;

async fn parse_test_command(input: &[u8]) -> ParsedResponse {
    let c = Cursor::new(input);
    let mut buf_reader = BufReader::new(c);
    parse_command(&mut buf_reader).await.unwrap()
}

async fn execute_test_command(input: ParsedResponse, store: Arc<DB>) -> Vec<u8> {
    let buf: Vec<u8> = Vec::new();
    let mut c = Cursor::new(buf);
    let mut buf_writer = BufWriter::new(&mut c);
    execute_commands(input, &store, &mut buf_writer)
        .await
        .unwrap();
    buf_writer.flush().await.unwrap();

    c.get_ref().to_vec()
}

/* NOTE: Easily copyable format type shi

#[tokio::test]
async fn test_execute_~_simple() {
    //Input
    let input = b"";

    // DB SETUP
    let store = Arc::new(DB::new(tempfile::tempdir().unwrap().path()).unwrap());

    // DB Shenanigans

    // Cmd parse and execute
    let cmd = parse_test_command(input).await;
    let r = execute_test_command(cmd, store).await;

    // Assert
    assert_eq!(r, b"");
}

*/

#[tokio::test]
async fn test_execute_rm_simple() {
    //Input
    let input = b"*2\r\n$2\r\nRM\r\n$3\r\nkey\r\n";

    // DB SETUP
    let store = Arc::new(DB::new(tempfile::tempdir().unwrap().path()).unwrap());

    // DB Shenanigans
    store.set_raw(b"key", b"value", None).unwrap();

    // Cmd parse and execute
    let cmd = parse_test_command(input).await;
    let r = execute_test_command(cmd, store).await;

    // Assert
    assert_eq!(r, b"+OK\r\n");
}

#[tokio::test]
async fn test_execute_rm_key_not_found() {
    //Input
    let input = b"*2\r\n$2\r\nRM\r\n$3\r\nkey\r\n";

    // DB SETUP
    let store = Arc::new(DB::new(tempfile::tempdir().unwrap().path()).unwrap());

    // DB Shenanigans

    // Cmd parse and execute
    let cmd = parse_test_command(input).await;
    let r = execute_test_command(cmd, store).await;

    // Assert
    assert_eq!(r, b"-ERR Sled Transaction failed\n\r\n");
}

#[tokio::test]
async fn test_execute_ping() {
    //Input
    let input = b"*1\r\n$4\r\nPING\r\n";

    // DB SETUP
    let store = Arc::new(DB::new(tempfile::tempdir().unwrap().path()).unwrap());

    // DB Shenanigans

    // Cmd parse and execute
    let cmd = parse_test_command(input).await;
    let r = execute_test_command(cmd, store).await;

    // Assert
    assert_eq!(r, b"+PONG\r\n");
}

#[tokio::test]
async fn test_execute_get_key_not_found() {
    //Input
    let input = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";

    // DB SETUP
    let store = Arc::new(DB::new(tempfile::tempdir().unwrap().path()).unwrap());

    // DB Shenanigans
    
    // Cmd parse and execute
    let cmd = parse_test_command(input).await;
    let r = execute_test_command(cmd, store).await;

    assert_eq!(r, b"$-1\r\n");
}

#[tokio::test]
async fn test_execute_get_simple() {
    //Input
    let input = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";

    // DB SETUP
    let store = Arc::new(DB::new(tempfile::tempdir().unwrap().path()).unwrap());

    // DB Shenanigans
    store.set_raw(b"key", b"value", None).unwrap();

    // Cmd parse and execute
    let cmd = parse_test_command(input).await;
    let r = execute_test_command(cmd, store).await;

    assert_eq!(r, b"$5\r\nvalue\r\n");
}

#[tokio::test]
async fn test_execute_get_binary() {
    //Input
    let input = b"*2\r\n$3\r\nGET\r\n$3\r\nbin\r\n";

    // DB SETUP
    let store = Arc::new(DB::new(tempfile::tempdir().unwrap().path()).unwrap());

    // DB Shenanigans
    store.set_raw(b"bin", b"\xDE\xAD\xBE\xEF", None).unwrap();

    // Cmd parse and execute
    let cmd = parse_test_command(input).await;
    let r = execute_test_command(cmd, store).await;

    assert_eq!(r, b"$4\r\n\xDE\xAD\xBE\xEF\r\n");
}

#[tokio::test]
async fn test_execute_set_simple() {
    //Input
    let input = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";

    // DB SETUP
    let store = Arc::new(DB::new(tempfile::tempdir().unwrap().path()).unwrap());

    // DB Shenanigans

    // Cmd parse and execute
    let cmd = parse_test_command(input).await;
    let r = execute_test_command(cmd, store.clone()).await;

    // Assert
    assert_eq!(store.get("key").unwrap().unwrap(), "value");
    assert_eq!(r, b"+OK\r\n");
}

#[tokio::test]
async fn test_execute_set_ttl() {
    //Input
    let input = b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$1\r\n5\r\n";

    // DB SETUP
    let store = Arc::new(DB::new(tempfile::tempdir().unwrap().path()).unwrap());

    // DB Shenanigans

    // Cmd parse and execute
    let cmd = parse_test_command(input).await;
    let r = execute_test_command(cmd, store.clone()).await;

    // Assert
    assert_eq!(store.get("key").unwrap().unwrap(), "value");
    assert_eq!(r, b"+OK\r\n");

    sleep(Duration::new(6, 0)).await;

    assert_eq!(store.get("key").unwrap(), None);
}


