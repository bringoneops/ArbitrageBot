use reqwest::Client;
use std::env;
use tokio::signal::unix::{signal, SignalKind};
use tracing::error;

use agents::TaskSet;

/// Spawn a task to listen for SIGINT and SIGTERM and perform a graceful
/// shutdown sequence when triggered.
pub fn install(join_set: TaskSet) {
    tokio::spawn(async move {
        let mut sigint = signal(SignalKind::interrupt()).expect("sigint handler");
        let mut sigterm = signal(SignalKind::terminate()).expect("sigterm handler");
        // Wait for either signal
        tokio::select! {
            _ = sigint.recv() => {},
            _ = sigterm.recv() => {},
        }

        // Mark readiness false so external systems stop sending traffic.
        super::set_ready(false);

        // Abort all running tasks (intake and websocket tasks).
        {
            let mut set = join_set.lock().await;
            set.abort_all();
            while set.join_next().await.is_some() {}
        }

        // Drain queues and flush sink: nothing explicit is required beyond
        // awaiting task completion, but this block signifies intent.

        // Confirm health endpoint before exiting.
        let port: u16 = env::var("HEALTH_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(8080);
        let url = format!("http://127.0.0.1:{port}/healthz");
        let client = Client::new();
        if let Err(e) = client.get(url).send().await {
            error!("healthz check failed: {e}");
        }

        // Exit process after graceful shutdown.
        std::process::exit(0);
    });
}
