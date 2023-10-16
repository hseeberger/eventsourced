//! [EvtLog](eventsourced::EvtLog) and [SnapshotStore](eventsourced::SnapshotStore) implementations
//! based upon [NATS](https://nats.io/).

mod evt_log;
mod snapshot_store;

pub use evt_log::{Config as NatsEvtLogConfig, NatsEvtLog};
pub use snapshot_store::{Config as NatsSnapshotStoreConfig, NatsSnapshotStore};

use eventsourced::TrySeqNoFromZero;
use prost::{DecodeError, EncodeError};
use std::error::Error as StdError;
use thiserror::Error;

/// Errors from the [NatsEvtLog] or [NatsSnapshotStore].
#[derive(Debug, Error)]
pub enum Error {
    #[error("NATS error: {0}")]
    Nats(String, #[source] Box<dyn std::error::Error + Send + Sync>),

    /// Event cannot be converted into bytes.
    #[error("cannot convert event to bytes")]
    EvtsIntoBytes(#[source] Box<dyn StdError + Send + Sync + 'static>),

    /// Bytes cannot be converted to event.
    #[error("cannot convert bytes to event")]
    EvtsFromBytes(#[source] Box<dyn StdError + Send + Sync + 'static>),

    /// Snapshot cannot be encoded as Protocol Buffers.
    #[error("cannot encode snapshot as Protocol Buffers")]
    EncodeSnapshot(#[from] EncodeError),

    /// Snapshot cannot be decoded from Protocol Buffers.
    #[error("cannot decode snapshot from Protocol Buffers")]
    DecodeSnapshot(#[from] DecodeError),

    /// Invalid sequence number.
    #[error("invalid sequence number")]
    InvalidSeqNo(#[source] TrySeqNoFromZero),
}

#[cfg(test)]
pub mod tests {
    pub const NATS_VERSION: &str = "2.10.3";
}
