//! [EventLog](eventsourced::event_log::EventLog) and
//! [SnapshotStore](eventsourced::snapshot_store::SnapshotStore) implementations based upon [PostgreSQL](https://www.postgresql.org/).

#![warn(missing_docs)]

mod event_log;
mod snapshot_store;

pub use event_log::{Config as PostgresEventLogConfig, PostgresEventLog};
pub use snapshot_store::{Config as PostgresSnapshotStoreConfig, PostgresSnapshotStore};

use bb8_postgres::{
    PostgresConnectionManager,
    bb8::{Pool, PooledConnection},
};
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
    ToBytes(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),

    /// Cannot convert bytes to an event.
    #[error("cannot convert bytes to an event")]
    FromBytes(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),

    /// Sequence number must not be zero.
    #[error("sequence number must not be zero")]
    ZeroNonZeroU64,

    /// Sequence number must not be zero.
    #[error("invalid last sequence number: {0:?} {1:?}")]
    InvalidLastNonZeroU64(Option<NonZeroU64>, Option<NonZeroU64>),
}

#[cfg(test)]
mod tests {
    pub(crate) fn compose_image(service: &str) -> (&'static str, &'static str) {
        const COMPOSE: &str = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../docker-compose.yaml"
        ));

        let header = format!("  {service}:");

        COMPOSE
            .lines()
            .skip_while(|line| line.trim_end() != header)
            .skip(1)
            .take_while(|line| line.starts_with("    "))
            .find_map(|line| line.trim().strip_prefix("image:"))
            .map(|image| image.trim().trim_matches('"'))
            .and_then(|image| image.rsplit_once(':'))
            .unwrap_or_else(|| panic!("no image for service {service} in docker-compose.yaml"))
    }
}
