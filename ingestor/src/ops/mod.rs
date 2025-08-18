use anyhow::Result;

mod health;
#[cfg(feature = "prometheus-exporter")]
mod metrics;

pub use health::set_ready;

pub fn serve_all(metrics_enabled: bool) -> Result<()> {
    if metrics_enabled {
        #[cfg(feature = "prometheus-exporter")]
        metrics::serve()?;
    }
    health::serve();
    Ok(())
}
