use anyhow::Result;
use eventsourced::EventSourced;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug)]
pub struct Counter;

impl EventSourced for Counter {
    type Id = Uuid;
    type Cmd = Cmd;
    type Evt = Evt;
    type State = State;
    type Error = Error;
    type ReplyInfo = ();
    type Reply = u64;

    const TYPE_NAME: &'static str = "counter";

    fn handle_cmd(
        id: &Self::Id,
        state: &Self::State,
        cmd: Self::Cmd,
    ) -> Result<(Self::Evt, Self::ReplyInfo), Self::Error> {
        let id = *id;
        let value = state.value;

        match cmd {
            Cmd::Increase(inc) if inc > u64::MAX - value => Err(Error::Overflow { value, inc }),
            Cmd::Increase(inc) => Ok((Evt::Increased { id, inc }, ())),

            Cmd::Decrease(dec) if dec > value => Err(Error::Underflow { value, dec }),
            Cmd::Decrease(dec) => Ok((Evt::Decreased { id, dec }, ())),
        }
    }

    fn handle_evt(mut state: Self::State, evt: Self::Evt) -> Self::State {
        match evt {
            Evt::Increased { inc, .. } => state.value += inc,
            Evt::Decreased { dec, .. } => state.value -= dec,
        };
        state
    }

    fn make_reply(_reply_info: Self::ReplyInfo, state: &Self::State) -> Self::Reply {
        state.value
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cmd {
    Increase(u64),
    Decrease(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Evt {
    Increased { id: Uuid, inc: u64 },
    Decreased { id: Uuid, dec: u64 },
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
