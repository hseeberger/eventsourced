use eventsourced::{Cmd, EventSourced};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct Counter(u64);

impl EventSourced for Counter {
    type Id = Uuid;
    type Evt = CounterEvt;

    const TYPE_NAME: &'static str = "counter";

    fn handle_evt(self, evt: &CounterEvt) -> Self {
        match evt {
            CounterEvt::Increased(_, n) => Self(self.0 + n),
            CounterEvt::Decreased(_, n) => Self(self.0 - n),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CounterEvt {
    Increased(Uuid, u64),
    Decreased(Uuid, u64),
}

#[derive(Debug)]
pub struct IncreaseCounter(pub u64);

impl Cmd<Counter> for IncreaseCounter {
    type Error = Overflow;
    type Reply = u64;

    fn handle_cmd(&self, id: &Uuid, state: &Counter) -> Result<CounterEvt, Self::Error> {
        if u64::MAX - state.0 < self.0 {
            Err(Overflow)
        } else {
            Ok(CounterEvt::Increased(*id, self.0))
        }
    }

    fn make_reply(&self, _id: &Uuid, state: &Counter, _evt: CounterEvt) -> Self::Reply {
        state.0
    }
}

#[derive(Debug)]
pub struct Overflow;

#[derive(Debug)]
pub struct DecreaseCounter(pub u64);

impl Cmd<Counter> for DecreaseCounter {
    type Error = Underflow;
    type Reply = u64;

    fn handle_cmd(&self, id: &Uuid, state: &Counter) -> Result<CounterEvt, Self::Error> {
        if state.0 < self.0 {
            Err(Underflow)
        } else {
            Ok(CounterEvt::Decreased(*id, self.0))
        }
    }

    fn make_reply(&self, _id: &Uuid, state: &Counter, _evt: CounterEvt) -> Self::Reply {
        state.0
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Underflow;
