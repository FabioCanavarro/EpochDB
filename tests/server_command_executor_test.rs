use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use epoch_db::DB;
use epoch_db::server::commands::ParsedResponse;
use epoch_db::server::{
    execute_commands,
    parse_command
};
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

#[tokio::test]
async fn test_execute_get_metadata_no_ttl() {
    //Input
    let input = b"*2\r\n$12\r\nGET_METADATA\r\n$3\r\nkey\r\n";

    // DB SETUP
    let store = Arc::new(DB::new(tempfile::tempdir().unwrap().path()).unwrap());

    // DB Shenanigans
    store.set_raw(b"key", b"val", None).unwrap();

    // Cmd parse and execute
    let cmd = parse_test_command(input).await;
    let r = execute_test_command(cmd, store.clone()).await;

    // Assert
    assert_eq!(
        r,
        format!(
            "*6\r\n$9\r\nfrequency\r\n:0\r\n$10\r\ncreated_at\r\n:{}\r\n$3\r\nttl\r\n$-1\r\n",
            store.get_metadata("key").unwrap().unwrap().created_at
        )
        .as_bytes()
    );
}

#[tokio::test]
async fn test_execute_get_metadata_simple() {
    //Input
    let input = b"*2\r\n$12\r\nGET_METADATA\r\n$3\r\nkey\r\n";

    // DB SETUP
    let store = Arc::new(DB::new(tempfile::tempdir().unwrap().path()).unwrap());

    // DB Shenanigans
    store.set_raw(b"key", b"val", Some(Duration::from_secs(60))).unwrap();

    // Cmd parse and execute
    let cmd = parse_test_command(input).await;
    let r = execute_test_command(cmd, store.clone()).await;

    // Assert
    assert_eq!(
        r,
        format!(
            "*6\r\n$9\r\nfrequency\r\n:0\r\n$10\r\ncreated_at\r\n:{}\r\n$3\r\nttl\r\n:{}\r\n",
            store.get_metadata("key").unwrap().unwrap().created_at,
            store.get_metadata("key").unwrap().unwrap().ttl.unwrap()
        )
        .as_bytes()
    );
}

#[tokio::test]
async fn test_execute_get_metadata_key_not_found() {
    //Input
    let input = b"*2\r\n$12\r\nGET_METADATA\r\n$3\r\nkey\r\n";

    // DB SETUP
    let store = Arc::new(DB::new(tempfile::tempdir().unwrap().path()).unwrap());

    // DB Shenanigans

    // Cmd parse and execute
    let cmd = parse_test_command(input).await;
    let r = execute_test_command(cmd, store.clone()).await;

    // Assert
    assert_eq!(
        r,
        b"$-1\r\n"
    );
}

#[tokio::test]
async fn test_execute_flush_simple() {
    //Input
    let input = b"*1\r\n$5\r\nFLUSH\r\n";

    // DB SETUP
    let store = Arc::new(DB::new(tempfile::tempdir().unwrap().path()).unwrap());

    // DB Shenanigans

    // Cmd parse and execute
    let cmd = parse_test_command(input).await;
    let r = execute_test_command(cmd, store).await;

    // Assert
    assert_eq!(r, b"+OK\r\n");
}

#[tokio::test]
async fn test_execute_size_no_data() {
    //Input
    let input = b"*1\r\n$4\r\nSIZE\r\n";

    // DB SETUP
    let store = Arc::new(DB::new(tempfile::tempdir().unwrap().path()).unwrap());

    // DB Shenanigans

    // Cmd parse and execute
    let cmd = parse_test_command(input).await;
    let r = execute_test_command(cmd, store).await;

    // Assert
    assert_eq!(r, b":0\r\n");
}

#[tokio::test]
async fn test_execute_size_simple() {
    //Input
    let input = b"*1\r\n$4\r\nSIZE\r\n";

    // DB SETUP
    let store = Arc::new(DB::new(tempfile::tempdir().unwrap().path()).unwrap());

    // DB Shenanigans
    store.set_raw(b"a", b"1", None).unwrap();
    store.set_raw(b"b", b"2", None).unwrap();
    // Cmd parse and execute
    let cmd = parse_test_command(input).await;
    let r = execute_test_command(cmd, store).await;

    // Assert
    assert_eq!(r, b":2\r\n");
}

#[tokio::test]
async fn test_execute_increment_frequency_simple() {
    //Input
    let input = b"*2\r\n$19\r\nINCREMENT_FREQUENCY\r\n$3\r\nkey\r\n";

    // DB SETUP
    let store = Arc::new(DB::new(tempfile::tempdir().unwrap().path()).unwrap());

    // DB Shenanigans
    store.set_raw(b"key", b"value", None).unwrap();

    // Cmd parse and execute
    let cmd = parse_test_command(input).await;
    let r = execute_test_command(cmd, store.clone()).await;

    // Assert
    assert_eq!(r, b"+OK\r\n");
    assert_eq!(store.get_metadata_raw(b"key").unwrap().unwrap().freq, 1);
}

#[tokio::test]
async fn test_execute_increment_frequency_key_not_found() {
    //Input
    let input = b"*2\r\n$19\r\nINCREMENT_FREQUENCY\r\n$3\r\nkey\r\n";

    // DB SETUP
    let store = Arc::new(DB::new(tempfile::tempdir().unwrap().path()).unwrap());

    // DB Shenanigans

    // Cmd parse and execute
    let cmd = parse_test_command(input).await;
    let r = execute_test_command(cmd, store.clone()).await;

    // Assert
    assert_eq!(r, b"$-1\r\n");
}

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

#[tokio::test]
async fn test_execute_invalid_command() {
    //Input
    let input = b"*2\r\n$6\r\nFOOBAR\r\n$3\r\nkey\r\n";

    // DB SETUP
    let store = Arc::new(DB::new(tempfile::tempdir().unwrap().path()).unwrap());

    // Cmd parse and execute
    let cmd = parse_test_command(input).await;
    let r = execute_test_command(cmd, store).await;

    // Assert
    assert_eq!(r, b"-ERR Command is invalid\r\n");
}

#[tokio::test]
async fn test_execute_set_too_few_argument() {
    //input
    let input = b"*2\r\n$3\r\nSET\r\n$3\r\nkey\r\n";

    // db setup
    let store = Arc::new(DB::new(tempfile::tempdir().unwrap().path()).unwrap());

    // cmd parse and execute
    let cmd = parse_test_command(input).await;
    let r = execute_test_command(cmd, store).await;

    // assert
    assert_eq!(r, b"-ERR Wrong number of arguments for \"set\" command; Needed at least 3 arguments, Received 2 arguments\r\n");
}

#[tokio::test]
async fn test_execute_set_too_many_argument() {
    //input
    let input = b"*5\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n";

    // db setup
    let store = Arc::new(DB::new(tempfile::tempdir().unwrap().path()).unwrap());

    // cmd parse and execute
    let cmd = parse_test_command(input).await;
    let r = execute_test_command(cmd, store).await;

    // assert
    assert_eq!(r, b"-ERR Wrong number of arguments for \"set\" command; Needed at least 3 arguments, Received 5 arguments\r\n");
}
