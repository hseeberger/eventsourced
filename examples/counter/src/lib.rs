pub mod counter;

use crate::counter::{Cmd, Counter};
use anyhow::{Context, Result};
use eventsourced::{binarize, evt_log::EvtLog, snapshot_store::SnapshotStore, EventSourcedExt};
use serde::Deserialize;
use std::{iter, num::NonZeroUsize, time::Instant};
use tokio::task::JoinSet;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    entity_count: usize,
    evt_count: usize,
}

pub async fn run<L, S>(config: Config, evt_log: L, snapshot_store: S) -> Result<()>
where
    L: EvtLog<Id = Uuid>,
    S: SnapshotStore<Id = Uuid>,
{
    let ids = iter::repeat_with(Uuid::now_v7)
        .take(config.entity_count)
        .collect::<Vec<_>>();

    println!("Spawning and sending a lot of commands ...");
    let mut tasks = JoinSet::new();
    let start_time = Instant::now();
    for id in ids.clone() {
        let evt_log = evt_log.clone();
        let snapshot_store = snapshot_store.clone();
        let counter = Counter::spawn(
            id,
            None,
            NonZeroUsize::new(42).expect("42 is not zero"),
            evt_log,
            snapshot_store,
            binarize::serde_json::SerdeJsonBinarize,
        )
        .await
        .context("spawn counter entity")?;

        tasks.spawn(async move {
            for n in 0..config.evt_count / 2 {
                if n > 0 && n % 2_500 == 0 {
                    println!("{id}: {} events persisted", n * 2);
                }
                counter
                    .handle_cmd(Cmd::Increase(n as u64))
                    .await
                    .context("send/receive Inc command")
                    .unwrap()
                    .context("handle Inc command")
                    .unwrap();
                counter
                    .handle_cmd(Cmd::Decrease(n as u64))
                    .await
                    .context("send/receive Dec command")
                    .unwrap()
                    .context("handle Dec command")
                    .unwrap();
            }
        });
    }

    while tasks.join_next().await.is_some() {}
    let end_time = Instant::now();
    println!(
        "Duration for spawning {} entities and sending {} commands to each: {:?}",
        config.entity_count,
        config.evt_count,
        end_time - start_time
    );

    println!("Spawning the above entities again ...");
    let mut tasks = JoinSet::new();
    let start_time = Instant::now();
    for id in ids {
        let evt_log = evt_log.clone();
        let snapshot_store = snapshot_store.clone();
        tasks.spawn(async move {
            let _counter = Counter::spawn(
                id,
                None,
                NonZeroUsize::new(42).expect("42 is not zero"),
                evt_log,
                snapshot_store,
                binarize::serde_json::SerdeJsonBinarize,
            )
            .await
            .unwrap();
        });
    }
    while tasks.join_next().await.is_some() {}
    let end_time = Instant::now();

    println!(
        "Duration for spawning {} entities with {} events each: {:?}",
        config.entity_count,
        config.evt_count,
        end_time - start_time
    );

    Ok(())
}
