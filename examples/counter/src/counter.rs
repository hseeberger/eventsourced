use anyhow::Result;
use eventsourced::{EventSourced, IntoTaggedEvt};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Default)]
pub struct Counter {
    value: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cmd {
    Inc(u64),
    Dec(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Evt {
    Increased(u64),
    Decreased(u64),
}

#[derive(Debug, Clone, Copy, Error)]
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

    const TYPE_NAME: &'static str = "counter";

    /// Command handler, returning the to be persisted event or an error.
    fn handle_cmd(
        &self,
        _id: Uuid,
        cmd: Self::Cmd,
    ) -> Result<impl IntoTaggedEvt<Self::Evt>, Self::Error> {
        let value = self.value;

        match cmd {
            Cmd::Inc(inc) if inc > u64::MAX - value => Err(Error::Overflow { value, inc }),
            Cmd::Inc(inc) => Ok(Evt::Increased(inc)),

            Cmd::Dec(dec) if dec > value => Err(Error::Underflow { value, dec }),
            Cmd::Dec(dec) => Ok(Evt::Decreased(dec)),
        }
    }

    /// Event handler, also returning whether to take a snapshot or not.
    fn handle_evt(&mut self, evt: Self::Evt) -> Option<Self::State> {
        match evt {
            Evt::Increased(inc) => self.value += inc,
            Evt::Decreased(dec) => self.value -= dec,
        }

        // No snapshots.
        None
    }

    fn set_state(&mut self, _state: Self::State) {
        // This method cannot be called as long as `handle_evt` always returns `None`.
        panic!("impossible: no snapshots");
    }
}
