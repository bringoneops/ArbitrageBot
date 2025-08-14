use anyhow::{anyhow, Result};
use futures::{future, Future};
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::{sync::mpsc, task::JoinHandle};

async fn good_init(
    task_tx: mpsc::UnboundedSender<JoinHandle<()>>,
    counter: Arc<AtomicUsize>,
) -> Result<()> {
    let c = counter.clone();
    let handle = tokio::spawn(async move {
        c.fetch_add(1, Ordering::SeqCst);
    });
    let _ = task_tx.send(handle);
    Ok(())
}

async fn bad_init(_task_tx: mpsc::UnboundedSender<JoinHandle<()>>) -> Result<()> {
    Err(anyhow!("init failed"))
}

#[tokio::test]
async fn continues_when_one_exchange_fails() {
    let (task_tx, mut task_rx) = mpsc::unbounded_channel::<JoinHandle<()>>();
    let counter = Arc::new(AtomicUsize::new(0));

    let futures: Vec<Pin<Box<dyn Future<Output = (&'static str, Result<()>)> + Send>>> = vec![
        {
            let tx = task_tx.clone();
            let counter = counter.clone();
            Box::pin(async move { ("ok", good_init(tx, counter).await) })
        },
        {
            let tx = task_tx.clone();
            Box::pin(async move { ("bad", bad_init(tx).await) })
        },
    ];

    let results = future::join_all(futures).await;

    let mut ok = 0;
    let mut err = 0;
    for (name, res) in results {
        match res {
            Ok(_) => {
                if name == "ok" {
                    ok += 1;
                }
            }
            Err(_) => {
                if name == "bad" {
                    err += 1;
                }
            }
        }
    }

    assert_eq!(ok, 1);
    assert_eq!(err, 1);

    drop(task_tx);
    while let Some(handle) = task_rx.recv().await {
        handle.await.unwrap();
    }

    assert_eq!(counter.load(Ordering::SeqCst), 1);
}
