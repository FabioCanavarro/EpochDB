#![allow(unused_parens)]

use std::error::Error;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use epoch_db::DB;
use epoch_db::server::response_handler;
use epoch_db::server::utils::init_logger;
use tokio::net::TcpListener;
use tokio::runtime::Builder;
use tokio::spawn;
use tokio::time::sleep;
use tracing::{
    error,
    info,
    warn
};

// Cli Parser
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[arg(short, long, default_value_t = ("127.0.0.1:8080".to_string()) )]
    addr: String,

    #[arg(short, long, default_value_t = ("./".to_string()) )]
    path: String,

    #[arg(short, long)]
    workers: Option<usize>,

    /// Set the logging verbosity level "off" | "error" | "warn" | "info" |
    /// "debug" | "trace"
    #[arg(short, long, default_value_t = ("info".to_string()) )]
    verbosity: String // Accepts: "off", "error", "warn", "info", "debug", "trace"
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    init_logger(cli.verbosity.clone());

    let mut builder = Builder::new_multi_thread();

    builder.enable_all();

    if let Some(n) = cli.workers {
        builder.worker_threads(n);
    };

    let runtime = builder.build()?;

    runtime.block_on(async_main(cli))
}

async fn async_main(args: (Cli)) -> Result<(), Box<dyn Error>> {
    let mut counter: i8 = 0;

    let cli = args;

    let addr = cli.addr;
    let listener = TcpListener::bind(&addr).await?;
    info!("Listening to {}", addr);

    // TODO: LAZY STATIC DB
    let store = Arc::new(DB::new(&PathBuf::from(cli.path))?);

    loop {
        let stream_set = listener.accept().await;
        match stream_set {
            Ok(t) => {
                counter = 0;
                let _handler = spawn(response_handler(t.0, store.clone()));
            },
            Err(e) => {
                match e.kind() {
                    io::ErrorKind::ConnectionRefused => {
                        warn!("Connection Refused!!!");
                        continue;
                    },
                    io::ErrorKind::ConnectionAborted => {
                        warn!("Connection Aborted!!!");
                        continue;
                    },
                    _ => {
                        //TODO: test 10 times sleep 100ms, if error then break and log?
                        if counter < 10 {
                            error!("Error: {:?}", e);
                            warn!("Retry attempt: {:?}", counter);

                            counter += 1;
                            sleep(Duration::new(0, 100000000)).await;
                            continue;
                        } else {
                            error!("An Error occured: {:?}", e);
                            break;
                        }
                    }
                }
            }
        };
    }
    Ok(())
}

/* TODO:
 *   Add a REPL during the loop, so they can issue commands such as:
 *   - Backup, Load(seperated to 2?, one for overwrite one just adds non
 *     existing keys)
 *   - Check amount of tll keys
 *   - Check highest freq
 *   - Check amount of ttl keys
 *   - Be able to Get, Set, rm directly from server for speed
 *   - Use Prometheus instead of the exporter
 */
