use anyhow::{anyhow, Result};
use futures::{future, Future};
use std::{pin::Pin, sync::{atomic::{AtomicUsize, Ordering}, Arc}};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

async fn good_init(tasks: Arc<Mutex<JoinSet<()>>>, counter: Arc<AtomicUsize>) -> Result<()> {
    {
        let mut guard = tasks.lock().await;
        let c = counter.clone();
        guard.spawn(async move {
            c.fetch_add(1, Ordering::SeqCst);
        });
    }
    Ok(())
}

async fn bad_init(_tasks: Arc<Mutex<JoinSet<()>>>) -> Result<()> {
    Err(anyhow!("init failed"))
}

#[tokio::test]
async fn continues_when_one_exchange_fails() {
    let tasks = Arc::new(Mutex::new(JoinSet::new()));
    let counter = Arc::new(AtomicUsize::new(0));

    let futures: Vec<Pin<Box<dyn Future<Output = (&'static str, Result<()>)> + Send>>> = vec![
        {
            let tasks = tasks.clone();
            let counter = counter.clone();
            Box::pin(async move { ("ok", good_init(tasks, counter).await) })
        },
        {
            let tasks = tasks.clone();
            Box::pin(async move { ("bad", bad_init(tasks).await) })
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

    {
        let mut guard = tasks.lock().await;
        guard.join_next().await.unwrap().unwrap();
    }

    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

