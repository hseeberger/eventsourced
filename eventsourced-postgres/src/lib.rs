//! An [EvtLog](eventsourced::EvtLog) implementation based on [PostgreSQL](https://www.postgresql.org/).

#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]
#![feature(return_position_impl_trait_in_trait)]

mod evt_log;

pub use evt_log::*;
