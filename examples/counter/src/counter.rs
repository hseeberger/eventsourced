use eventsourced::{Command, CommandEffect, EventSourced};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct Counter(u64);

impl EventSourced for Counter {
    type Id = Uuid;
    type Event = CounterEvent;

    const TYPE_NAME: &'static str = "counter";

    fn handle_event(self, event: CounterEvent) -> Self {
        match event {
            CounterEvent::Increased(_, n) => Self(self.0 + n),
            CounterEvent::Decreased(_, n) => Self(self.0 - n),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CounterEvent {
    Increased(Uuid, u64),
    Decreased(Uuid, u64),
}

#[derive(Debug)]
pub struct IncreaseCounter(pub u64);

impl Command<Counter> for IncreaseCounter {
    type Error = Overflow;
    type Reply = u64;

    fn handle_command(self, id: &Uuid, state: &Counter) -> CommandEffect<Counter, u64, Overflow> {
        if u64::MAX - state.0 < self.0 {
            CommandEffect::reject(Overflow)
        } else {
            CommandEffect::emit_and_reply(
                CounterEvent::Increased(*id, self.0),
                |state: &Counter| state.0,
            )
        }
    }
}

#[derive(Debug)]
pub struct Overflow;

#[derive(Debug)]
pub struct DecreaseCounter(pub u64);

impl Command<Counter> for DecreaseCounter {
    type Error = Underflow;
    type Reply = u64;

    fn handle_command(self, id: &Uuid, state: &Counter) -> CommandEffect<Counter, u64, Underflow> {
        if state.0 < self.0 {
            CommandEffect::reject(Underflow)
        } else {
            CommandEffect::emit_and_reply(
                CounterEvent::Decreased(*id, self.0),
                |state: &Counter| state.0,
            )
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Underflow;
