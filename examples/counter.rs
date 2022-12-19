use anyhow::{Context, Result};
use eventsourced::{
    evt_log::nats::{Config, NatsEvtLog},
    Entity, EventSourced,
};
#[cfg(any(feature = "serde_json", feature = "flexbuffers"))]
use serde::{Deserialize, Serialize};
use std::time::Instant;
use thiserror::Error;
use tokio::task::JoinSet;
use uuid::Uuid;

const ENTITY_COUNT: usize = 100;
const EVT_COUNT: usize = 5000;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Started ...");

    let evt_log = NatsEvtLog::new(Config::default()).await?;

    let mut tasks = JoinSet::new();
    let start_time = Instant::now();
    for _ in 1..=ENTITY_COUNT {
        let evt_log = evt_log.clone();
        let counter = Entity::spawn(Uuid::now_v7(), Counter(0), evt_log)
            .await
            .context("Cannot spawn entity")
            .unwrap();
        tasks.spawn(async move {
            for n in 1..=EVT_COUNT / 2 {
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
    while let Some(_) = tasks.join_next().await {}
    let end_time = Instant::now();

    println!(
        "Duration for {} entities with {} events each: {:?}",
        ENTITY_COUNT,
        EVT_COUNT,
        end_time - start_time
    );

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cmd {
    Inc(u64),
    Dec(u64),
}

#[cfg(any(feature = "serde_json", feature = "flexbuffers"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Evt {
    Increased { old_value: u64, inc: u64 },
    Decreased { old_value: u64, dec: u64 },
}

#[cfg(feature = "prost")]
include!(concat!(env!("OUT_DIR"), "/example.counter.rs"));

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
enum Error {
    #[error("Overflow: value={value}, increment={inc}")]
    Overflow { value: u64, inc: u64 },
    #[error("Underflow: value={value}, decrement={dec}")]
    Underflow { value: u64, dec: u64 },
}

#[derive(Debug)]
struct Counter(u64);

impl EventSourced for Counter {
    type Cmd = Cmd;

    type Evt = Evt;

    type Error = Error;

    fn handle_cmd(&self, cmd: Self::Cmd) -> Result<Vec<Self::Evt>, Self::Error> {
        match cmd {
            Cmd::Inc(inc) => {
                if inc > u64::MAX - self.0 {
                    Err(Error::Overflow { value: self.0, inc })
                } else {
                    #[cfg(any(feature = "serde_json", feature = "flexbuffers"))]
                    {
                        Ok(vec![Evt::Increased {
                            old_value: self.0,
                            inc,
                        }])
                    }
                    #[cfg(feature = "prost")]
                    Ok(vec![Evt {
                        evt: Some(evt::Evt::Increased(Increased {
                            old_value: self.0,
                            inc,
                        })),
                    }])
                }
            }
            Cmd::Dec(dec) => {
                if dec > self.0 {
                    Err(Error::Underflow { value: self.0, dec })
                } else {
                    #[cfg(any(feature = "serde_json", feature = "flexbuffers"))]
                    {
                        Ok(vec![Evt::Decreased {
                            old_value: self.0,
                            dec,
                        }])
                    }
                    #[cfg(feature = "prost")]
                    Ok(vec![Evt {
                        evt: Some(evt::Evt::Decreased(Decreased {
                            old_value: self.0,
                            dec,
                        })),
                    }])
                }
            }
        }
    }

    #[cfg(any(feature = "serde_json", feature = "flexbuffers"))]
    fn handle_evt(&mut self, evt: &Self::Evt) {
        match evt {
            Evt::Increased { old_value: _, inc } => self.0 += inc,
            Evt::Decreased { old_value: _, dec } => self.0 -= dec,
        }
    }

    #[cfg(feature = "prost")]
    fn handle_evt(&mut self, evt: &Self::Evt) {
        match evt.evt {
            Some(evt::Evt::Increased(Increased { old_value: _, inc })) => self.0 += inc,
            Some(evt::Evt::Decreased(Decreased { old_value: _, dec })) => self.0 -= dec,
            None => panic!("evt is a mandatory field"),
        }
    }
}
