#![allow(incomplete_features)]
#![feature(return_position_impl_trait_in_trait)]

pub mod counter;

use crate::counter::{Cmd, Counter};
use anyhow::{Context, Result};
use eventsourced::{convert, EventSourcedExt, EvtLog, SnapshotStore};
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
    L: EvtLog,
    S: SnapshotStore,
{
    let ids = iter::repeat(())
        .take(config.entity_count)
        .map(|_| Uuid::now_v7())
        .collect::<Vec<_>>();

    println!("Spawning and sending a lot of commands ...");
    let mut tasks = JoinSet::new();
    let start_time = Instant::now();
    for id in &ids {
        let id = *id;

        let evt_log = evt_log.clone();
        let snapshot_store = snapshot_store.clone();
        let counter = Counter::default();
        let counter = counter
            .spawn(
                id,
                unsafe { NonZeroUsize::new_unchecked(42) },
                evt_log,
                snapshot_store,
                convert::serde_json::binarizer(),
            )
            .await
            .context("Cannot spawn entity")?;

        tasks.spawn(async move {
            for n in 0..config.evt_count / 2 {
                if n > 0 && n % 2_500 == 0 {
                    println!("{id}: {} events persisted", n * 2);
                }
                counter
                    .handle_cmd(Cmd::Inc(n as u64))
                    .await
                    .context("Cannot handle Inc command")
                    .unwrap()
                    .context("Invalid command")
                    .unwrap();
                counter
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
            let _counter = Counter::default()
                .spawn(
                    id,
                    unsafe { NonZeroUsize::new_unchecked(42) },
                    evt_log,
                    snapshot_store,
                    convert::serde_json::binarizer(),
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
        config.entity_count,
        config.evt_count,
        end_time - start_time
    );

    Ok(())
}
