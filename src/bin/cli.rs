use clap::Parser;
use epoch_db::client::cli::{
    Cli,
    Client
};
use epoch_db::client::{
    handle_response,
    tcp_logic
};
use epoch_db::db::errors::TransientError;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Return the helping description if they didnt specify any arguments
    if cli.command.is_none() {
        Cli::parse_from(["kvs", "--help"]);
        return;
    }

    // WARN: Single connection only, wastes a lot, we can have a repl later?
    //
    // Bind to the address
    let res = match TcpStream::connect(&cli.addr).await {
        Ok(stream) => {
            let client = Client {
                stream,
                buf: Vec::new()
            };
            tcp_logic(cli, client).await
        },
        Err(e) => {
            Err(e).map_err(|e| {
                TransientError::IOError {
                    error: e
                }
            })
        },
    };

    if let Err(e) = handle_response(res).await {
        println!("Error: {}", e)
    }
}
