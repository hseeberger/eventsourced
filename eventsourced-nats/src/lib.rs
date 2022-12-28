//! [EvtLog] and [SnapshotStore] implementations based on [NATS](https://nats.io/).

#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]
#![feature(return_position_impl_trait_in_trait)]

mod evt_log;
mod snapshot_store;

pub use evt_log::*;
pub use snapshot_store::*;
