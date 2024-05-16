//! [EventLog](eventsourced::event_log::EventLog) and
//! [SnapshotStore](eventsourced::snapshot_store::SnapshotStore) implementations based upon [PostgreSQL](https://www.postgresql.org/).

mod event_log;
mod snapshot_store;

pub use event_log::{Config as PostgresEventLogConfig, PostgresEventLog};
pub use snapshot_store::{Config as PostgresSnapshotStoreConfig, PostgresSnapshotStore};

use bb8_postgres::{
    bb8::{Pool, PooledConnection},
    PostgresConnectionManager,
};
use error_ext::BoxError;
use std::num::NonZeroU64;
use thiserror::Error;

type CnnPool<T> = Pool<PostgresConnectionManager<T>>;

type Cnn<'a, T> = PooledConnection<'a, PostgresConnectionManager<T>>;

/// Errors from the [PostgresEventLog] or [PostgresSnapshotStore].
#[derive(Debug, Error)]
pub enum Error {
    /// Postgres error.
    #[error("Postgres error: {0}")]
    Postgres(String, #[source] tokio_postgres::Error),

    /// Cannot get connection from pool.
    #[error("cannot get connection from pool")]
    GetConnection(#[source] bb8_postgres::bb8::RunError<tokio_postgres::Error>),

    /// Cannot convert an event to bytes.
    #[error("cannot convert an event to bytes")]
    ToBytes(#[source] BoxError),

    /// Cannot convert bytes to an event.
    #[error("cannot convert bytes to an event")]
    FromBytes(#[source] BoxError),

    /// Unexpected sequence number.
    #[error("expected sequence number {0:?}, but was {1:?}")]
    UnexpectedSeqNo(Option<NonZeroU64>, Option<NonZeroU64>),

    /// Sequence number must not be zero.
    #[error("sequence number must not be zero")]
    ZeroSeqNo,

    /// To be persisted events must not be empty.
    #[error("to be persisted events must not be empty")]
    EmptyEvents,
}
