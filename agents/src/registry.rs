use std::sync::Arc;

use anyhow::Result;
use arb_core as core;
use dashmap::DashMap;
use futures::future::BoxFuture;
use reqwest::Client;
use rustls::ClientConfig;
use tokio::sync::mpsc;

use crate::TaskSet;

pub type AdapterFactory = Arc<
    dyn Fn(
            &'static core::config::Config,
            &core::config::ExchangeConfig,
            Client,
            TaskSet,
            Arc<DashMap<String, mpsc::Sender<core::events::StreamMessage<'static>>>>,
            Arc<ClientConfig>,
            usize,
        ) -> BoxFuture<'static, Result<Vec<mpsc::Receiver<core::events::StreamMessage<'static>>>>>
        + Send
        + Sync,
>;

use once_cell::sync::Lazy;

static REGISTRY: Lazy<DashMap<&'static str, AdapterFactory>> = Lazy::new(|| DashMap::new());

pub fn register_adapter(id: &'static str, factory: AdapterFactory) {
    REGISTRY.insert(id, factory);
}

pub fn get_adapter(id: &str) -> Option<AdapterFactory> {
    REGISTRY.get(id).map(|f| f.value().clone())
}
