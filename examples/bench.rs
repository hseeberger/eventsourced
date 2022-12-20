use crate::counter::{Cmd, Counter};
use anyhow::{Context, Result};
use eventsourced::{
    evt_log::nats::{Config as NatsEvtLogConfig, NatsEvtLog},
    snapshot_store::nats::{Config as NatsSnapshotStoreConfig, NatsSnapshotStore},
    Entity,
};
use std::{iter, time::Instant};
use tokio::task::JoinSet;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use uuid::Uuid;

const ENTITY_COUNT: usize = 20;
const EVT_COUNT: usize = 50000;
const SNAPSHOT_AFTER: u64 = 20000;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .try_init()
        .context("Cannot initialize tracing")?;

    let evt_log = NatsEvtLog::new(NatsEvtLogConfig::default()).await?;
    let snapshot_store = NatsSnapshotStore::new(NatsSnapshotStoreConfig::default()).await?;

    let ids = iter::repeat(())
        .take(ENTITY_COUNT)
        .map(|_| Uuid::now_v7())
        .collect::<Vec<_>>();

    println!("Writing ...");
    let mut tasks = JoinSet::new();
    let start_time = Instant::now();
    for id in &ids {
        let evt_log = evt_log.clone();
        let snapshot_store = snapshot_store.clone();
        let counter = Counter::default().with_snapshot_after(SNAPSHOT_AFTER);
        let counter = Entity::spawn(*id, counter, evt_log, snapshot_store)
            .await
            .context("Cannot spawn entity")?;
        tasks.spawn(async move {
            for n in 0..EVT_COUNT / 2 {
                let _ = counter
                    .handle_cmd(Cmd::Inc(n as u64))
                    .await
                    .context("Cannot handle Inc command")
                    .unwrap();
                let _ = counter
                    .handle_cmd(Cmd::Dec(n as u64))
                    .await
                    .context("Cannot handle Dec command")
                    .unwrap();
            }
        });
    }
    while tasks.join_next().await.is_some() {}
    let end_time = Instant::now();
    println!(
        "Duration for writing {} entities with {} events each: {:?}",
        ENTITY_COUNT,
        EVT_COUNT,
        end_time - start_time
    );

    println!("Reading ...");
    let mut tasks = JoinSet::new();
    let start_time = Instant::now();
    for id in ids {
        let evt_log = evt_log.clone();
        let snapshot_store = snapshot_store.clone();
        tasks.spawn(async move {
            let _counter = Entity::spawn(id, Counter::default(), evt_log, snapshot_store)
                .await
                .context("Cannot spawn entity")
                .unwrap();
        });
    }
    while tasks.join_next().await.is_some() {}
    let end_time = Instant::now();

    println!(
        "Duration for reading {} entities with {} events each: {:?}",
        ENTITY_COUNT,
        EVT_COUNT,
        end_time - start_time
    );

    Ok(())
}

mod counter {
    include!("counter.rs");
}
