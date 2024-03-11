use eventsourced::{Cmd, EventSourced};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct Counter(u64);

impl EventSourced for Counter {
    type Id = Uuid;
    type Evt = Evt;

    fn handle_evt(self, evt: Evt) -> Self {
        match evt {
            Evt::Increased(_, n) => Self(self.0 + n),
            Evt::Decreased(_, n) => Self(self.0 - n),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Evt {
    Increased(Uuid, u64),
    Decreased(Uuid, u64),
}

#[derive(Debug)]
pub struct Increase(pub u64);

impl Cmd for Increase {
    type EventSourced = Counter;
    type Error = Overflow;
    type Reply = u64;

    fn handle_cmd(self, id: &Uuid, state: &Counter) -> Result<Evt, Self::Error> {
        if u64::MAX - state.0 < self.0 {
            Err(Overflow)
        } else {
            Ok(Evt::Increased(*id, self.0))
        }
    }

    fn reply(state: &Counter) -> Self::Reply {
        state.0
    }
}

#[derive(Debug)]
pub struct Overflow;

#[derive(Debug)]
pub struct Decrease(pub u64);

impl Cmd for Decrease {
    type EventSourced = Counter;
    type Error = Underflow;
    type Reply = u64;

    fn handle_cmd(self, id: &Uuid, state: &Counter) -> Result<Evt, Self::Error> {
        if state.0 < self.0 {
            Err(Underflow)
        } else {
            Ok(Evt::Decreased(*id, self.0))
        }
    }

    fn reply(state: &Counter) -> Self::Reply {
        state.0
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Underflow;
