use crate::counter::{Cmd, Counter};
use anyhow::{Context, Result};
use eventsourced::{convert, Entity, EvtLog, SnapshotStore};
use serde::Deserialize;
use std::{iter, time::Instant};
use tokio::task::JoinSet;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    entity_count: usize,
    evt_count: usize,
    snapshot_after: u64,
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
        let counter = Counter::default().with_snapshot_after(config.snapshot_after);
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
            for n in 0..config.evt_count / 2 {
                if n > 0 && n % 2_500 == 0 {
                    println!("{id}: {} events persisted", n * 2);
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
        config.entity_count,
        config.evt_count,
        end_time - start_time
    );

    Ok(())
}

pub mod counter {
    include!(concat!(env!("OUT_DIR"), "/counter.rs"));

    use anyhow::Result;
    use eventsourced::EventSourced;
    use thiserror::Error;
    use tracing::debug;

    #[derive(Debug, Default)]
    pub struct Counter {
        value: u64,
        snapshot_after: Option<u64>,
    }

    impl Counter {
        pub fn with_snapshot_after(self, snapshot_after: u64) -> Self {
            Self {
                snapshot_after: Some(snapshot_after),
                ..Default::default()
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Cmd {
        Inc(u64),
        Dec(u64),
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
    pub enum Error {
        #[error("Overflow: value={value}, increment={inc}")]
        Overflow { value: u64, inc: u64 },
        #[error("Underflow: value={value}, decrement={dec}")]
        Underflow { value: u64, dec: u64 },
    }

    impl EventSourced for Counter {
        type Cmd = Cmd;

        type Evt = Evt;

        type State = u64;

        type Error = Error;

        fn handle_cmd(&self, cmd: Self::Cmd) -> Result<Vec<Self::Evt>, Self::Error> {
            match cmd {
                Cmd::Inc(inc) => {
                    if inc > u64::MAX - self.value {
                        Err(Error::Overflow {
                            value: self.value,
                            inc,
                        })
                    } else {
                        Ok(vec![Evt {
                            evt: Some(evt::Evt::Increased(Increased {
                                old_value: self.value,
                                inc,
                            })),
                        }])
                    }
                }
                Cmd::Dec(dec) => {
                    if dec > self.value {
                        Err(Error::Underflow {
                            value: self.value,
                            dec,
                        })
                    } else {
                        Ok(vec![Evt {
                            evt: Some(evt::Evt::Decreased(Decreased {
                                old_value: self.value,
                                dec,
                            })),
                        }])
                    }
                }
            }
        }

        fn handle_evt(&mut self, seq_no: u64, evt: &Self::Evt) -> Option<Self::State> {
            match evt.evt {
                Some(evt::Evt::Increased(Increased { old_value, inc })) => {
                    self.value += inc;
                    debug!(seq_no, old_value, inc, value = self.value, "Increased");
                }
                Some(evt::Evt::Decreased(Decreased { old_value, dec })) => {
                    self.value -= dec;
                    debug!(seq_no, old_value, dec, value = self.value, "Decreased");
                }
                None => panic!("evt is a mandatory field"),
            }

            self.snapshot_after.and_then(|snapshot_after| {
                if seq_no % snapshot_after == 0 {
                    Some(self.value)
                } else {
                    None
                }
            })
        }

        fn set_state(&mut self, state: Self::State) {
            self.value = state;
            debug!(value = self.value, "Set state");
        }
    }
}
