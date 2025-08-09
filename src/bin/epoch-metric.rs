use std::future::ready;

use axum::{Router, routing::get};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let recorder_handle = setup_prometheus_metric_recorder().await;

    let app = Router::new().route("/metrics", get(move || ready(recorder_handle.render())));

    println!("Binding to port 3000");
    let listener = TcpListener::bind("0.0.0.0:3001").await.unwrap();

    println!("Serving...");
    axum::serve(listener, app).await.unwrap();
}

async fn setup_prometheus_metric_recorder() -> PrometheusHandle {
    PrometheusBuilder::new().install_recorder().unwrap()
}
