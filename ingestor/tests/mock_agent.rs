use agents::adapter::ExchangeAdapter;
use arb_core::events::{Event, StreamMessage, TradeEvent};
use canonical::MdEvent;
use std::borrow::Cow;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

struct MockAdapter {
    tx: mpsc::Sender<StreamMessage<'static>>,
}

#[async_trait::async_trait]
impl ExchangeAdapter for MockAdapter {
    async fn subscribe(&mut self) -> anyhow::Result<()> { Ok(()) }
    async fn run(&mut self) -> anyhow::Result<()> {
        let trade = TradeEvent {
            event_time: 1,
            symbol: "BTCUSD".to_string(),
            trade_id: 1,
            price: Cow::Borrowed("100.0"),
            quantity: Cow::Borrowed("1.0"),
            buyer_order_id: 1,
            seller_order_id: 2,
            trade_time: 1,
            buyer_is_maker: false,
            best_match: true,
        };
        let msg = StreamMessage { stream: "btcusd@trade".into(), data: Event::Trade(trade) };
        self.tx.send(msg).await.unwrap();
        Ok(())
    }
    async fn heartbeat(&mut self) -> anyhow::Result<()> { Ok(()) }
    async fn auth(&mut self) -> anyhow::Result<()> { Ok(()) }
    async fn backfill(&mut self) -> anyhow::Result<()> { Ok(()) }
}

#[tokio::test]
async fn mock_agent_pipeline() {
    let (tx, mut rx) = mpsc::channel::<StreamMessage<'static>>(8);
    let out: Arc<Mutex<Vec<MdEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let out_clone = out.clone();
    let handle = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if let Ok(ev) = MdEvent::try_from(msg.data) {
                out_clone.lock().await.push(ev);
            }
        }
    });
    let mut adapter = MockAdapter { tx };
    adapter.run().await.unwrap();
    drop(adapter);
    handle.await.unwrap();
    let events = out.lock().await;
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], MdEvent::Trade(_)));
}
