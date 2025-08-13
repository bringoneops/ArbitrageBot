use anyhow::{anyhow, Result};
use once_cell::sync::OnceCell;
use serde::Deserialize;
use std::env;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub proxy_url: Option<String>,
    pub spot_symbols: Vec<String>,
    pub futures_symbols: Vec<String>,
    pub chunk_size: usize,
    pub event_buffer_size: usize,
    pub enable_spot: bool,
    pub enable_futures: bool,
    pub enable_metrics: bool,
}

static CONFIG: OnceCell<Config> = OnceCell::new();

impl Config {
    pub fn from_env() -> Result<Self> {
        let proxy_url = env::var("SOCKS5_PROXY").ok();
        let spot_symbols = env::var("SPOT_SYMBOLS")
            .unwrap_or_default()
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        let futures_symbols = env::var("FUTURES_SYMBOLS")
            .unwrap_or_default()
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        let chunk_size = env::var("CHUNK_SIZE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(100);
        let event_buffer_size = env::var("EVENT_BUFFER_SIZE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(1024);
        let enable_spot = env::var("ENABLE_SPOT")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(true);
        let enable_futures = env::var("ENABLE_FUTURES")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(true);
        let enable_metrics = env::var("ENABLE_METRICS")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(true);
        Ok(Config {
            proxy_url,
            spot_symbols,
            futures_symbols,
            chunk_size,
            event_buffer_size,
            enable_spot,
            enable_futures,
            enable_metrics,
        })
    }

    pub fn validate(&self) -> Result<()> {
        if self.enable_spot && self.spot_symbols.is_empty() {
            return Err(anyhow!("spot symbol list cannot be empty"));
        }
        if self.enable_futures && self.futures_symbols.is_empty() {
            return Err(anyhow!("futures symbol list cannot be empty"));
        }
        if self.chunk_size == 0 || self.chunk_size > 1024 {
            return Err(anyhow!("chunk_size must be between 1 and 1024"));
        }
        if self.event_buffer_size == 0 || self.event_buffer_size > 65536 {
            return Err(anyhow!("event_buffer_size must be between 1 and 65536"));
        }
        Ok(())
    }
}

pub fn load() -> Result<&'static Config> {
    let cfg = Config::from_env()?;
    cfg.validate()?;
    Ok(CONFIG.get_or_init(|| cfg))
}

pub fn get() -> &'static Config {
    CONFIG.get().expect("config not loaded")
}

pub fn metrics_enabled() -> bool {
    CONFIG.get().map(|c| c.enable_metrics).unwrap_or(false)
}
