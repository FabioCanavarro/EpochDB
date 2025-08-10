use epoch_db::DB;
use std::{path::Path, sync::Arc, thread::{self, sleep}, time::Duration};
use axum::{extract::State, routing::get, Router};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tokio::net::TcpListener;

// Function to open the server
#[tokio::main]
async fn server_main() {
    let recorder_handle = Arc::new(setup_prometheus_metric_recorder());

    let app = Router::new().route("/metrics", get(metrics_handler)).with_state(recorder_handle);

    let listener = TcpListener::bind("0.0.0.0:3001").await.unwrap();

    axum::serve(listener, app).await.unwrap();
}

fn setup_prometheus_metric_recorder() -> PrometheusHandle {
    PrometheusBuilder::new().install_recorder().unwrap()
}

async fn metrics_handler(State(state): State<Arc<PrometheusHandle>>) -> String {
    state.render()
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
    thread::spawn(server_main);

    // Let the thread build the app first
    // and Let prometheus scrape once
    sleep(Duration::new(15, 0));

    let db = DB::new(Path::new("./databasetest")).unwrap();

    db.set("H", "haha", None).unwrap();
    db.set("HAHAHHAH", "Skib", None).unwrap();
    db.set("HI", "h", None).unwrap();
    db.set("Chronos", "Temporal", None).unwrap();
    db.set("pop", "HAHAHAHH", Some(Duration::new(0, 100000))).unwrap();
    for i in 0..1000 {
        db.get("HI").unwrap();
        db.set(&format!("{i}"), "h", None).unwrap();
    }
    db.backup_to(Path::new("./backup/")).unwrap();

    Ok(())
}

// TODO: Add more comments to this example
