use axum::{extract::State, http::StatusCode, routing::get, Router};
use once_cell::sync::Lazy;
use std::{
    env,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tracing::error;

static READY: Lazy<Arc<AtomicBool>> = Lazy::new(|| Arc::new(AtomicBool::new(true)));

pub fn set_ready(ready: bool) {
    READY.store(ready, Ordering::SeqCst);
}

async fn readyz(State(state): State<Arc<AtomicBool>>) -> StatusCode {
    if state.load(Ordering::SeqCst) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

pub fn serve() {
    let port: u16 = env::var("HEALTH_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);
    let state = READY.clone();
    tokio::spawn(async move {
        let app = Router::new()
            .route("/healthz", get(|| async { StatusCode::OK }))
            .route("/readyz", get(readyz))
            .with_state(state);
        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        if let Err(e) = axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
        {
            error!("health server error: {e}");
        }
    });
}
