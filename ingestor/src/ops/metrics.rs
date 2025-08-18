use anyhow::{Context, Result};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::{env, net::SocketAddr};

pub fn serve() -> Result<()> {
    let port: u16 = env::var("METRICS_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(9000);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()
        .context("installing Prometheus exporter")?;
    Ok(())
}
