//! [EvtLog](eventsourced::EvtLog) and [SnapshotStore](eventsourced::SnapshotStore) implementations
//! based upon [PostgreSQL](https://www.postgresql.org/).

#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]
#![feature(nonzero_min_max)]
#![feature(return_position_impl_trait_in_trait)]

mod evt_log;
mod snapshot_store;

pub use evt_log::{Config as PostgresEvtLogConfig, PostgresEvtLog};
pub use snapshot_store::{Config as PostgresSnapshotStoreConfig, PostgresSnapshotStore};

use bb8_postgres::{
    bb8::{Pool, PooledConnection},
    PostgresConnectionManager,
};
use thiserror::Error;

type CnnPool<T> = Pool<PostgresConnectionManager<T>>;

type Cnn<'a, T> = PooledConnection<'a, PostgresConnectionManager<T>>;

/// Errors from the [PostgresEvtLog] or [PostgresSnapshotStore].
#[derive(Debug, Error)]
pub enum Error {
    /// Cannot create connection manager.
    #[error("Cannot create connection manager")]
    ConnectionManager(#[source] tokio_postgres::Error),

    /// Cannot create connection pool.
    #[error("Cannot create connection pool")]
    ConnectionPool(#[source] tokio_postgres::Error),

    /// Cannot get connection from pool.
    #[error("Cannot get connection from pool")]
    GetConnection(#[source] bb8_postgres::bb8::RunError<tokio_postgres::Error>),

    /// Cannot prepare statement.
    #[error("Cannot prepare statement")]
    PrepareStmt(#[source] tokio_postgres::Error),

    /// Cannot start transaction.
    #[error("Cannot start transaction")]
    StartTx(#[source] tokio_postgres::Error),

    /// Cannot commit transaction.
    #[error("Cannot commit transaction")]
    CommitTx(#[source] tokio_postgres::Error),

    /// Cannot execute statement.
    #[error("Cannot execute statement")]
    ExecuteStmt(#[source] tokio_postgres::Error),

    /// Cannot convert events to bytes.
    #[error("Cannot convert events to bytes")]
    ToBytes(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),

    /// Cannot convert bytes to events.
    #[error("Cannot convert bytes to events")]
    FromBytes(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),

    /// Cannot get next row.
    #[error("Cannot get next row")]
    NextRow(#[source] tokio_postgres::Error),

    /// Cannot get column as Uuid.
    #[error("Cannot get column as Uuid")]
    ColumnAsUuid(#[source] tokio_postgres::Error),
}
