use anyhow::Result;

/// Initialize the metrics exporter based on enabled features.
pub fn init_exporter() -> Result<()> {
    #[cfg(feature = "prometheus-exporter")]
    {
        use metrics_exporter_prometheus::PrometheusBuilder;
        // Install Prometheus exporter with default configuration.
        PrometheusBuilder::new().install()?;
    }

    #[cfg(feature = "datadog-exporter")]
    {
        use metrics_exporter_dogstatsd::DogStatsDBuilder;
        // Install Datadog (DogStatsD) exporter with default configuration.
        DogStatsDBuilder::default().install()?;
    }

    Ok(())
}
