//! [EventLog](eventsourced::event_log::EventLog) and
//! [SnapshotStore](eventsourced::snapshot_store::SnapshotStore) implementations based upon [PostgreSQL](https://www.postgresql.org/).

mod event_log;
mod pool;
mod snapshot_store;

pub use event_log::{Config as PostgresEventLogConfig, PostgresEventLog};
pub use snapshot_store::{Config as PostgresSnapshotStoreConfig, PostgresSnapshotStore};
