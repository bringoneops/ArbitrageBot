use std::sync::Arc;
use tokio::sync::Mutex;

use async_trait::async_trait;

use ingestor::adapter::ExchangeAdapter;

struct MockAdapter {
    calls: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait]
impl ExchangeAdapter for MockAdapter {
    async fn subscribe(&mut self) -> anyhow::Result<()> {
        self.calls.lock().await.push("subscribe");
        Ok(())
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        self.calls.lock().await.push("run");
        Ok(())
    }

    async fn heartbeat(&mut self) -> anyhow::Result<()> {
        self.calls.lock().await.push("heartbeat");
        Ok(())
    }

    async fn auth(&mut self) -> anyhow::Result<()> {
        self.calls.lock().await.push("auth");
        Ok(())
    }

    async fn backfill(&mut self) -> anyhow::Result<()> {
        self.calls.lock().await.push("backfill");
        Ok(())
    }
}

#[tokio::test]
async fn adapter_trait_is_callable() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut adapter = MockAdapter { calls: calls.clone() };

    adapter.auth().await.unwrap();
    adapter.backfill().await.unwrap();
    adapter.subscribe().await.unwrap();
    adapter.heartbeat().await.unwrap();
    adapter.run().await.unwrap();

    let calls = calls.lock().await.clone();
    assert_eq!(
        calls,
        vec!["auth", "backfill", "subscribe", "heartbeat", "run"]
    );
}

