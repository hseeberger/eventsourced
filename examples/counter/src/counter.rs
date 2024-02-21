use eventsourced::{CmdResult, CommandResult, EventSourced};
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
                CommandResult::reply_err(reply, Error::Overflow { value, inc })
            }
            Cmd::Inc(inc, reply) => {
                CommandResult::emit_and_reply(Evt::Increased(inc), reply, value + inc)
            }
            Cmd::Dec(dec, reply) if dec > value => {
                CommandResult::reply_err(reply, Error::Underflow { value, dec })
            }
            Cmd::Dec(dec, reply) => {
                CommandResult::emit_and_reply(Evt::Decreased(dec), reply, value - dec)
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

#[derive(Debug)]
pub enum Cmd {
    Inc(u64, CmdResult<u64, Error>),
    Dec(u64, CmdResult<u64, Error>),
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
