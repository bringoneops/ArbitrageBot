use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use agents::{
    run_adapters,
    spawn_adapters,
    ChannelRegistry,
    ExchangeAdapter,
    TaskSet,
    BINANCE_EXCHANGES,
    registry,
};
use arb_core as core;
use async_trait::async_trait;
use reqwest::Client;
use rustls::{ClientConfig, RootCertStore};
use tokio::{sync::{mpsc, Mutex}, task::JoinSet};
use serial_test::serial;

struct TestAdapter {
    counter: Arc<AtomicUsize>,
}

#[async_trait]
impl ExchangeAdapter for TestAdapter {
    async fn subscribe(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    async fn run(&mut self) -> anyhow::Result<()> {
        self.counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn heartbeat(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    async fn auth(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    async fn backfill(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
#[serial]
async fn spawn_adapters_ok() {
    std::env::set_var("API_KEY", "k");
    std::env::set_var("API_SECRET", "s");
    std::env::set_var("EXCHANGES", "test");

    // Build a config from the environment and leak it to obtain a 'static reference.
    let cfg = core::config::Config::from_env().unwrap();
    let cfg: &'static core::config::Config = Box::leak(Box::new(cfg));

    // Register a dummy adapter that returns a single receiver.
    registry::register_adapter(
        "test",
        Arc::new(|_, _, _, _, _, _| {
            Box::pin(async {
                let (_tx, rx) = mpsc::channel(1);
                Ok(vec![rx])
            })
        }),
    );

    let client = Client::new();
    let tls_config = Arc::new(
        ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth(),
    );

    let task_set: TaskSet = Arc::new(Mutex::new(JoinSet::new()));
    let channels = ChannelRegistry::new(cfg.event_buffer_size);

    assert!(
        spawn_adapters(cfg, client, task_set, channels, tls_config)
            .await
            .is_ok()
    );
}

#[tokio::test]
#[serial]
async fn spawn_adapters_returns_err_when_no_adapters_started() {
    std::env::set_var("API_KEY", "k");
    std::env::set_var("API_SECRET", "s");
    std::env::set_var("EXCHANGES", "unknown");

    let cfg = core::config::Config::from_env().unwrap();
    let cfg: &'static core::config::Config = Box::leak(Box::new(cfg));

    let client = Client::new();
    let tls_config = Arc::new(
        ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth(),
    );
    let task_set: TaskSet = Arc::new(Mutex::new(JoinSet::new()));
    let channels = ChannelRegistry::new(cfg.event_buffer_size);

    assert!(
        spawn_adapters(cfg, client, task_set, channels, tls_config)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn run_adapters_executes() {
    let counter = Arc::new(AtomicUsize::new(0));
    let adapter = TestAdapter {
        counter: counter.clone(),
    };
    run_adapters(vec![adapter]).await.unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[test]
fn binance_exchanges_present() {
    assert!(!BINANCE_EXCHANGES.is_empty());
}
