//! [EventLog](eventsourced::event_log::EventLog) and
//! [SnapshotStore](eventsourced::snapshot_store::SnapshotStore) implementations based upon [PostgreSQL](https://www.postgresql.org/).

pub mod event_log;
pub mod snapshot_store;

mod pool;
