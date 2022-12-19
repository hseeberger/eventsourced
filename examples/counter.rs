// This is a library example to be included into default (binary) examples!

use anyhow::Result;
use eventsourced::EventSourced;
#[cfg(any(feature = "serde_json", feature = "flexbuffers"))]
use serde::{Deserialize, Serialize};
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

#[cfg(any(feature = "serde_json", feature = "flexbuffers"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Evt {
    Increased { old_value: u64, inc: u64 },
    Decreased { old_value: u64, dec: u64 },
}

#[cfg(feature = "prost")]
include!(concat!(env!("OUT_DIR"), "/example.counter.rs"));

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
                    #[cfg(any(feature = "serde_json", feature = "flexbuffers"))]
                    {
                        Ok(vec![Evt::Increased {
                            old_value: self.value,
                            inc,
                        }])
                    }
                    #[cfg(feature = "prost")]
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
                    #[cfg(any(feature = "serde_json", feature = "flexbuffers"))]
                    {
                        Ok(vec![Evt::Decreased {
                            old_value: self.value,
                            dec,
                        }])
                    }
                    #[cfg(feature = "prost")]
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

    #[cfg(any(feature = "serde_json", feature = "flexbuffers"))]
    fn handle_evt(&mut self, seq_no: u64, evt: &Self::Evt) -> Option<Self::State> {
        match evt {
            Evt::Increased { old_value, inc } => {
                self.value += inc;
                debug!(old_value, inc, value = self.value, "Increased")
            }
            Evt::Decreased { old_value, dec } => {
                self.value -= dec;
                debug!(old_value, dec, value = self.value, "Decreased")
            }
        }

        self.snapshot_after.and_then(|snapshot_after| {
            if seq_no % snapshot_after == 0 {
                Some(self.value)
            } else {
                None
            }
        })
    }

    #[cfg(feature = "prost")]
    fn handle_evt(&mut self, seq_no: u64, evt: &Self::Evt) -> Option<Self::State> {
        match evt.evt {
            Some(evt::Evt::Increased(Increased { old_value, inc })) => {
                self.value += inc;
                debug!(seq_no, old_value, inc, value = self.value, "Increased")
            }
            Some(evt::Evt::Decreased(Decreased { old_value, dec })) => {
                self.value -= dec;
                debug!(seq_no, old_value, dec, value = self.value, "Decreased")
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
