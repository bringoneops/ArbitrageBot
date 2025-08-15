use agents::ChannelRegistry;

#[tokio::test]
async fn channels_only_created_when_subscribed() {
    let registry = ChannelRegistry::new(1);
    assert_eq!(registry.len(), 0);
    // Querying a channel that hasn't been subscribed does not create it
    assert!(registry.get("Test:BTCUSDT").is_none());
    assert_eq!(registry.len(), 0);

    // Subscribing creates the channel and yields a receiver
    let (_, rx) = registry.get_or_create("Test:BTCUSDT");
    assert!(rx.is_some());
    assert_eq!(registry.len(), 1);

    // Subsequent subscriptions reuse the existing channel
    let (_, rx2) = registry.get_or_create("Test:BTCUSDT");
    assert!(rx2.is_none());
    assert_eq!(registry.len(), 1);
}
