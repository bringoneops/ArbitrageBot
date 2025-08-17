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
pub struct ExchangeConfig {
    pub id: String,
    #[serde(default)]
    pub symbols: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub enum Symbols {
    All,
    List(Vec<String>),
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub proxy_url: Option<String>,
    pub spot_symbols: Symbols,
    pub futures_symbols: Symbols,
    pub mexc_symbols: Symbols,
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
    /// SHA-256 certificate pins encoded as hexadecimal strings.
    pub cert_pins: Vec<String>,
    pub exchanges: Vec<ExchangeConfig>,
}

static CONFIG: OnceCell<Config> = OnceCell::new();

/* ---------- Business-rule predicates (DECOMPOSE CONDITIONAL) ---------- */

fn has_valid_credentials(c: &Credentials) -> bool {
    !c.api_key.is_empty() && !c.api_secret.is_empty()
}

fn is_missing_required_symbols(enabled: bool, symbols: &Symbols) -> bool {
    matches!((enabled, symbols), (true, Symbols::List(list)) if list.is_empty())
}

fn rate_limits_present(http_burst: u32, http_refill: u32, ws_burst: u32, ws_refill: u32) -> bool {
    [http_burst, http_refill, ws_burst, ws_refill]
        .iter()
        .all(|&v| v > 0)
}

/* -------------------- helpers to avoid nested conditionals -------------------- */

fn creds_from_env() -> Option<Credentials> {
    let api_key = env::var("API_KEY").ok()?;
    let api_secret = env::var("API_SECRET").ok()?;
    let c = Credentials {
        api_key,
        api_secret,
    };
    has_valid_credentials(&c).then_some(c)
}

fn creds_from_file(path: &str) -> Result<Option<Credentials>> {
    let mut content = fs::read(path).context("reading credentials file")?;
    let creds: Credentials = from_slice(&mut content).context("parsing credentials file")?;
    Ok(has_valid_credentials(&creds).then_some(creds))
}

fn load_credentials() -> Result<Credentials> {
    if let Some(c) = creds_from_env() {
        return Ok(c);
    }
    if let Ok(path) = env::var("API_CREDENTIALS_FILE") {
        if let Some(c) = creds_from_file(&path)? {
            return Ok(c);
        }
    }
    Err(anyhow!(
        "API_KEY and API_SECRET must be set via env or credentials file"
    ))
}

/* -------------------------------- parsers -------------------------------- */

fn parse_symbols_env(var: &str) -> Symbols {
    match env::var(var) {
        Ok(v) if v.eq_ignore_ascii_case("ALL") => Symbols::All,
        Ok(v) => Symbols::List(
            v.split(',')
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect(),
        ),
        Err(_) => Symbols::List(Vec::new()),
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

/* --------------------------------- Config -------------------------------- */

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

        let exchange_ids = parse_list_env("EXCHANGES");
        let exchanges = exchange_ids
            .into_iter()
            .map(|id| {
                let var = format!("{}_SYMBOLS", id.to_uppercase());
                let symbols = match parse_symbols_env(&var) {
                    Symbols::All => Vec::new(),
                    Symbols::List(list) => list,
                };
                ExchangeConfig { id, symbols }
            })
            .collect();

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
            exchanges,
        })
    }

    fn validate_api_credentials(&self) -> Result<()> {
        if !has_valid_credentials(&self.credentials) {
            return Err(anyhow!("API credentials are required"));
        }
        Ok(())
    }

    fn ensure_in_range(&self, name: &str, value: usize, min: usize, max: usize) -> Result<()> {
        if !(min..=max).contains(&value) {
            return Err(anyhow!("{name} must be between {min} and {max}"));
        }
        Ok(())
    }

    fn validate_symbols(&self) -> Result<()> {
        let missing_spot = is_missing_required_symbols(self.enable_spot, &self.spot_symbols);
        let missing_futures =
            is_missing_required_symbols(self.enable_futures, &self.futures_symbols);
        let missing_mexc = is_missing_required_symbols(self.enable_mexc, &self.mexc_symbols);

        if missing_spot {
            return Err(anyhow!(
                "spot symbol list cannot be empty (set SPOT_SYMBOLS=ALL to use all symbols)"
            ));
        }
        if missing_futures {
            return Err(anyhow!(
                "futures symbol list cannot be empty (set FUTURES_SYMBOLS=ALL to use all symbols)"
            ));
        }
        if missing_mexc {
            return Err(anyhow!(
                "mexc symbol list cannot be empty (set MEXC_SYMBOLS=ALL to use all symbols)"
            ));
        }
        Ok(())
    }

    fn validate_sizes(&self) -> Result<()> {
        self.ensure_in_range("chunk_size", self.chunk_size, 1, 1024)?;
        self.ensure_in_range("event_buffer_size", self.event_buffer_size, 1, 65_536)?;
        Ok(())
    }

    fn validate_rate_limits(&self) -> Result<()> {
        let ok = rate_limits_present(
            self.http_burst,
            self.http_refill_per_sec,
            self.ws_burst,
            self.ws_refill_per_sec,
        );
        if !ok {
            return Err(anyhow!("rate limit values must be greater than zero"));
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        self.validate_api_credentials()?;
        self.validate_symbols()?;
        self.validate_sizes()?;
        self.validate_rate_limits()?;
        Ok(())
    }
}

/* ----------------------------- public API ----------------------------- */

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
