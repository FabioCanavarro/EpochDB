use axum::{routing::get, Router};
use tokio::net::TcpListener;


#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/metrics", get(async || {"HELLOOO"})); 

    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();

    axum::serve(listener, app).await.unwrap();
}


async fn get_metric() { }
