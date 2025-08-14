use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use agents::{run_adapters, spawn_adapters, ExchangeAdapter, BINANCE_EXCHANGES};
use arb_core as core;
use async_trait::async_trait;
use reqwest::Client;
use rustls::{ClientConfig, RootCertStore};
use tokio::{
    sync::{mpsc, Mutex},
    task::JoinSet,
};

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
async fn spawn_adapters_ok() {
    std::env::set_var("API_KEY", "k");
    std::env::set_var("API_SECRET", "s");
    std::env::set_var("ENABLE_SPOT", "0");
    std::env::set_var("ENABLE_FUTURES", "0");
    let cfg = core::config::load().unwrap();

    let client = Client::new();
    let tls_config = Arc::new(
        ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth(),
    );

    let tasks = Arc::new(Mutex::new(JoinSet::new()));
    let (tx, _rx) = mpsc::channel::<core::events::StreamMessage<'static>>(cfg.event_buffer_size);

    assert!(spawn_adapters(cfg, client, tasks, tx, tls_config)
        .await
        .is_ok());
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
