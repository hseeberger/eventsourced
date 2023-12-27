use anyhow::Result;
use eventsourced::EventSourced;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug)]
pub struct Counter;

impl EventSourced for Counter {
    type Id = String;
    type Cmd = Cmd;
    type Evt = Evt;
    type State = State;
    type Error = Error;

    const TYPE_NAME: &'static str = "counter";

    fn handle_cmd(
        _id: &Self::Id,
        state: &Self::State,
        cmd: Self::Cmd,
    ) -> Result<Self::Evt, Self::Error> {
        let value = state.value;

        match cmd {
            Cmd::Inc(inc) if inc > u64::MAX - value => Err(Error::Overflow { value, inc }),
            Cmd::Inc(inc) => Ok(Evt::Increased(inc)),

            Cmd::Dec(dec) if dec > value => Err(Error::Underflow { value, dec }),
            Cmd::Dec(dec) => Ok(Evt::Decreased(dec)),
        }
    }

    fn handle_evt(mut state: Self::State, evt: Self::Evt) -> Self::State {
        match evt {
            Evt::Increased(inc) => state.value += inc,
            Evt::Decreased(dec) => state.value -= dec,
        };
        state
    }
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

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct State {
    value: u64,
}

#[derive(Debug, Clone, Copy, Error)]
pub enum Error {
    #[error("Overflow: value={value}, increment={inc}")]
    Overflow { value: u64, inc: u64 },

    #[error("Underflow: value={value}, decrement={dec}")]
    Underflow { value: u64, dec: u64 },
}
