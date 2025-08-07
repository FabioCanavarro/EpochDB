use std::future::ready;

use axum::{Router, response::IntoResponse, routing::get};
use metrics_exporter_prometheus::{BuildError, PrometheusBuilder, PrometheusHandle};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let recorder_handle = setup_prometheus_metric_recorder().await;

    let app = Router::new().route("/metrics", get(move || ready(recorder_handle.render())));

    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();

    axum::serve(listener, app).await.unwrap();
}

async fn setup_prometheus_metric_recorder() -> PrometheusHandle {
    PrometheusBuilder::new().install_recorder().unwrap()
}
