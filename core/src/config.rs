use anyhow::{anyhow, Context, Result};
use once_cell::sync::OnceCell;
use serde::Deserialize;
use simd_json::serde::from_slice;
use std::{env, fs};

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
    pub mexc_symbols: Vec<String>,
    pub chunk_size: usize,
    pub event_buffer_size: usize,
    pub http_burst: u32,
    pub http_refill_per_sec: u32,
    pub ws_burst: u32,
    pub ws_refill_per_sec: u32,
    pub enable_spot: bool,
    pub enable_futures: bool,
    pub enable_mexc: bool,
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
        let creds: Credentials = from_slice(&mut content).context("parsing credentials file")?;
        if !creds.api_key.is_empty() && !creds.api_secret.is_empty() {
            return Ok(creds);
        }
    }

    Err(anyhow!(
        "API_KEY and API_SECRET must be set via env or credentials file"
    ))
}

fn parse_symbols_env(var: &str) -> Vec<String> {
    match env::var(var) {
        Ok(v) if v.eq_ignore_ascii_case("ALL") => Vec::new(),
        Ok(v) => v
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect(),
        Err(_) => Vec::new(),
    }
}

fn parse_usize_env(var: &str, default: usize) -> usize {
    env::var(var)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

fn parse_u32_env(var: &str, default: u32) -> u32 {
    env::var(var)
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(default)
}

fn parse_bool_env(var: &str, default: bool) -> bool {
    env::var(var)
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(default)
}

fn parse_list_env(var: &str) -> Vec<String> {
    env::var(var)
        .unwrap_or_default()
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let proxy_url = env::var("SOCKS5_PROXY").ok();
        let spot_symbols = parse_symbols_env("SPOT_SYMBOLS");
        let futures_symbols = parse_symbols_env("FUTURES_SYMBOLS");
        let mexc_symbols = parse_symbols_env("MEXC_SYMBOLS");
        let chunk_size = parse_usize_env("CHUNK_SIZE", 100);
        let event_buffer_size = parse_usize_env("EVENT_BUFFER_SIZE", 1024);
        let http_burst = parse_u32_env("HTTP_BURST", 10);
        let http_refill_per_sec = parse_u32_env("HTTP_REFILL_PER_SEC", 10);
        let ws_burst = parse_u32_env("WS_BURST", 5);
        let ws_refill_per_sec = parse_u32_env("WS_REFILL_PER_SEC", 5);
        let enable_spot = parse_bool_env("ENABLE_SPOT", true);
        let enable_futures = parse_bool_env("ENABLE_FUTURES", true);
        let enable_mexc = parse_bool_env("ENABLE_MEXC", false);
        let enable_metrics = parse_bool_env("ENABLE_METRICS", true);

        let credentials = load_credentials()?;
        let ca_bundle = env::var("CA_BUNDLE").ok();
        let cert_pins = parse_list_env("CERT_PINS");

        Ok(Config {
            proxy_url,
            spot_symbols,
            futures_symbols,
            mexc_symbols,
            chunk_size,
            event_buffer_size,
            http_burst,
            http_refill_per_sec,
            ws_burst,
            ws_refill_per_sec,
            enable_spot,
            enable_futures,
            enable_mexc,
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
        if self.enable_spot
            && self.spot_symbols.is_empty()
            && !env::var("SPOT_SYMBOLS")
                .unwrap_or_default()
                .eq_ignore_ascii_case("ALL")
        {
            return Err(anyhow!("spot symbol list cannot be empty"));
        }
        if self.enable_futures
            && self.futures_symbols.is_empty()
            && !env::var("FUTURES_SYMBOLS")
                .unwrap_or_default()
                .eq_ignore_ascii_case("ALL")
        {
            return Err(anyhow!("futures symbol list cannot be empty"));
        }
        if self.enable_mexc
            && self.mexc_symbols.is_empty()
            && !env::var("MEXC_SYMBOLS")
                .unwrap_or_default()
                .eq_ignore_ascii_case("ALL")
        {
            return Err(anyhow!("mexc symbol list cannot be empty"));
        }
        if self.chunk_size == 0 || self.chunk_size > 1024 {
            return Err(anyhow!("chunk_size must be between 1 and 1024"));
        }
        if self.event_buffer_size == 0 || self.event_buffer_size > 65536 {
            return Err(anyhow!("event_buffer_size must be between 1 and 65536"));
        }
        if self.http_burst == 0
            || self.http_refill_per_sec == 0
            || self.ws_burst == 0
            || self.ws_refill_per_sec == 0
        {
            return Err(anyhow!("rate limit values must be greater than zero"));
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
