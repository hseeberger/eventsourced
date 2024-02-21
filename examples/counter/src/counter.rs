use eventsourced::{CommandResult, EventSourced, Reply};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug)]
pub struct Counter;

impl EventSourced for Counter {
    type Id = String;
    type Cmd = Cmd;
    type Evt = Evt;
    type State = State;

    const TYPE_NAME: &'static str = "counter";

    fn handle_cmd(_id: &Self::Id, state: &Self::State, cmd: Self::Cmd) -> CommandResult<Self::Evt> {
        let value = state.value;

        match cmd {
            Cmd::Inc(inc, reply) if inc > u64::MAX - value => {
                reply.with(Err(Error::Overflow { value, inc }))
            }
            Cmd::Inc(inc, reply) => {
                CommandResult::emit(Evt::Increased(inc)).and_reply(reply, Ok(value + inc))
            }
            Cmd::Dec(dec, reply) if dec > value => reply.with(Err(Error::Underflow { value, dec })),
            Cmd::Dec(dec, reply) => {
                CommandResult::emit(Evt::Decreased(dec)).and_reply(reply, Ok(value - dec))
            }
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

#[derive(Debug, Clone, Copy)]
pub enum Cmd {
    Inc(u64, Reply<Result<u64, Error>>),
    Dec(u64, Reply<Result<u64, Error>>),
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
