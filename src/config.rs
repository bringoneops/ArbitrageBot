use anyhow::{anyhow, Context, Result};
use once_cell::sync::OnceCell;
use serde::Deserialize;
use std::{env, fs};
use simd_json::serde::from_slice;

#[derive(Clone, Deserialize)]
pub struct Credentials {
    pub api_key: String,
    pub api_secret: String,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("api_key", &"***redacted***")
            .field("api_secret", &"***redacted***")
            .finish()
    }
}

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
    pub credentials: Credentials,
    pub ca_bundle: Option<String>,
    pub cert_pins: Vec<String>,
}

static CONFIG: OnceCell<Config> = OnceCell::new();

fn load_credentials() -> Result<Credentials> {
    if let (Ok(api_key), Ok(api_secret)) = (env::var("API_KEY"), env::var("API_SECRET")) {
        if !api_key.is_empty() && !api_secret.is_empty() {
            return Ok(Credentials {
                api_key,
                api_secret,
            });
        }
    }

    if let Ok(path) = env::var("API_CREDENTIALS_FILE") {
        let mut content = fs::read(&path).context("reading credentials file")?;
        let creds: Credentials =
            from_slice(&mut content).context("parsing credentials file")?;
        if !creds.api_key.is_empty() && !creds.api_secret.is_empty() {
            return Ok(creds);
        }
    }

    Err(anyhow!(
        "API_KEY and API_SECRET must be set via env or credentials file"
    ))
}

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

        let credentials = load_credentials()?;
        let ca_bundle = env::var("CA_BUNDLE").ok();
        let cert_pins = env::var("CERT_PINS")
            .unwrap_or_default()
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();

        Ok(Config {
            proxy_url,
            spot_symbols,
            futures_symbols,
            chunk_size,
            event_buffer_size,
            enable_spot,
            enable_futures,
            enable_metrics,
            credentials,
            ca_bundle,
            cert_pins,
        })
    }

    pub fn validate(&self) -> Result<()> {
        if self.credentials.api_key.is_empty() || self.credentials.api_secret.is_empty() {
            return Err(anyhow!("API credentials are required"));
        }
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
    if cfg.enable_metrics {
        let _ = crate::metrics::init_exporter();
    }
    Ok(CONFIG.get_or_init(|| cfg))
}

pub fn get() -> &'static Config {
    CONFIG.get().expect("config not loaded")
}

pub fn metrics_enabled() -> bool {
    CONFIG.get().map(|c| c.enable_metrics).unwrap_or(false)
}
