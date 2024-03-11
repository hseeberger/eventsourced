pub mod counter;

use crate::counter::{Decrease, Increase};
use anyhow::{Context, Result};
use eventsourced::{binarize, Entity, EvtLog, SnapshotStore};
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
        let counter = Entity::new("counter", id)
            .add_cmd::<Increase>()
            .add_cmd::<Decrease>()
            .spawn(
                None,
                NonZeroUsize::new(2).expect("2 is not zero"),
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
                    .handle_cmd(Increase(n as u64))
                    .await
                    .expect("send/receive Inc command")
                    .expect("handle Inc command");
                counter
                    .handle_cmd(Decrease(n as u64))
                    .await
                    .expect("send/receive Dec command")
                    .expect("handle Dec command");
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
            let _ = Entity::new("counter", id)
                .add_cmd::<Increase>()
                .add_cmd::<Decrease>()
                .spawn(
                    None,
                    NonZeroUsize::new(2).expect("2 is not zero"),
                    evt_log,
                    snapshot_store,
                    binarize::serde_json::SerdeJsonBinarize,
                )
                .await
                .expect("spawn counter entity");
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
