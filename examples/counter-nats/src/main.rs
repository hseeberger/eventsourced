use crate::counter::{Cmd, Counter};
use anyhow::{anyhow, Context, Result};
use eventsourced::{convert, Entity, EvtLog, NoopSnapshotStore, SnapshotStore};
use eventsourced_nats::{NatsEvtLog, NatsEvtLogConfig};
use std::{iter, time::Instant};
use tokio::task::JoinSet;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use uuid::Uuid;

const ENTITY_COUNT: usize = 10;
const EVT_COUNT: usize = 1_000;
const SNAPSHOT_AFTER: u64 = u64::MAX;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .try_init()
        .context("Cannot initialize tracing")?;

    let evt_log = NatsEvtLog::new(NatsEvtLogConfig::default()).await?;
    evt_log
        .setup()
        .await
        .map_err(|error| anyhow!(error))
        .context("Cannot setup NatsEvtLog")?;

    let snapshot_store = NoopSnapshotStore;

    run(evt_log, snapshot_store).await
}

async fn run<L, S>(evt_log: L, snapshot_store: S) -> Result<()>
where
    L: EvtLog,
    S: SnapshotStore,
{
    let ids = iter::repeat(())
        .take(ENTITY_COUNT)
        .map(|_| Uuid::now_v7())
        .collect::<Vec<_>>();

    println!("Spawning and sending a lot of commands ...");
    let mut tasks = JoinSet::new();
    let start_time = Instant::now();
    for id in &ids {
        let id = *id;

        let evt_log = evt_log.clone();
        let snapshot_store = snapshot_store.clone();
        let counter = Counter::default().with_snapshot_after(SNAPSHOT_AFTER);
        let counter = Entity::spawn(
            id,
            counter,
            42,
            evt_log,
            snapshot_store,
            convert::prost::binarizer(),
        )
        .await
        .context("Cannot spawn entity")?;

        tasks.spawn(async move {
            for n in 0..EVT_COUNT / 2 {
                if n % 10_000 == 0 {
                    println!("{id}: {n} events persisted");
                }
                let _ = counter
                    .handle_cmd(Cmd::Inc(n as u64))
                    .await
                    .context("Cannot handle Inc command")
                    .unwrap()
                    .context("Invalid command")
                    .unwrap();
                let _ = counter
                    .handle_cmd(Cmd::Dec(n as u64))
                    .await
                    .context("Cannot handle Dec command")
                    .unwrap()
                    .context("Invalid command")
                    .unwrap();
            }
        });
    }

    while tasks.join_next().await.is_some() {}
    let end_time = Instant::now();
    println!(
        "Duration for spawning {} entities and sending {} commands to each: {:?}",
        ENTITY_COUNT,
        EVT_COUNT,
        end_time - start_time
    );

    println!("Spawning the above entities again ...");
    let mut tasks = JoinSet::new();
    let start_time = Instant::now();
    for id in ids {
        let evt_log = evt_log.clone();
        let snapshot_store = snapshot_store.clone();
        tasks.spawn(async move {
            let _counter = Entity::spawn(
                id,
                Counter::default(),
                42,
                evt_log,
                snapshot_store,
                convert::prost::binarizer(),
            )
            .await
            .context("Cannot spawn entity")
            .unwrap();
        });
    }
    while tasks.join_next().await.is_some() {}
    let end_time = Instant::now();

    println!(
        "Duration for spawning {} entities with {} events each: {:?}",
        ENTITY_COUNT,
        EVT_COUNT,
        end_time - start_time
    );

    Ok(())
}

mod counter {
    use eventsourced::EventSourced;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../include/counter.rs"
    ));
    include!(concat!(env!("OUT_DIR"), "/counter.rs"));
}
