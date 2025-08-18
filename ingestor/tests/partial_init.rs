use anyhow::{anyhow, Result};
use futures::{future, Future};
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::{sync::Mutex, task::JoinSet};

type InitFuture = Pin<Box<dyn Future<Output = (&'static str, Result<()>)> + Send>>;

async fn good_init(task_set: Arc<Mutex<JoinSet<()>>>, counter: Arc<AtomicUsize>) -> Result<()> {
    let c = counter.clone();
    {
        let mut set = task_set.lock().await;
        set.spawn(async move {
            c.fetch_add(1, Ordering::SeqCst);
        });
    }
    Ok(())
}

async fn bad_init(_task_set: Arc<Mutex<JoinSet<()>>>) -> Result<()> {
    Err(anyhow!("init failed"))
}

#[tokio::test]
async fn continues_when_one_exchange_fails() {
    let task_set = Arc::new(Mutex::new(JoinSet::new()));
    let counter = Arc::new(AtomicUsize::new(0));

    let futures: Vec<InitFuture> = vec![
        {
            let set = task_set.clone();
            let counter = counter.clone();
            Box::pin(async move { ("ok", good_init(set, counter).await) })
        },
        {
            let set = task_set.clone();
            Box::pin(async move { ("bad", bad_init(set).await) })
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
        let mut set = task_set.lock().await;
        while let Some(res) = set.join_next().await {
            res.unwrap();
        }
    }

    assert_eq!(counter.load(Ordering::SeqCst), 1);
}
