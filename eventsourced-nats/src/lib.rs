//! [EvtLog](eventsourced::EvtLog) and [SnapshotStore](eventsourced::SnapshotStore) implementations
//! based upon [NATS](https://nats.io/).

#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]
#![feature(nonzero_min_max)]
#![feature(return_position_impl_trait_in_trait)]

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
    /// The connection to the NATS server cannot be established.
    #[error("Cannot connect to NATS server")]
    Connect(#[from] std::io::Error),

    /// An event cannot be published.
    #[error("Cannot publish events")]
    PublishEvts(#[source] async_nats::Error),

    /// An ACK for publishing an event cannot be received.
    #[error("Cannot get ACK for publishing events")]
    PublishEvtsAck(#[source] async_nats::Error),

    /// A NATS stream cannot be obtained.
    #[error("Cannot get NATS stream")]
    GetStream(#[source] async_nats::Error),

    /// A NATS stream cannot be created.
    #[error("Cannot create NATS stream")]
    CreateStream(#[source] async_nats::Error),

    /// A NATS consumer cannot be created.
    #[error("Cannot create NATS consumer")]
    CreateConsumer(#[source] async_nats::Error),

    /// The message stream from a NATS consumer cannot be obtained.
    #[error("Cannot get message stream from NATS consumer")]
    GetMessages(#[source] async_nats::Error),

    /// A message cannot be obtained from the NATS message stream.
    #[error("Cannot get message from NATS message stream")]
    GetMessage(#[source] async_nats::Error),

    /// Cannot get message info.
    #[error("Cannot get message info")]
    GetMessageInfo(#[source] async_nats::Error),

    /// The last message for a NATS stream cannot be obtained.
    #[error("Cannot get last message for NATS stream")]
    GetLastMessage(#[source] async_nats::Error),

    /// A raw NATS message cannot be converted into a NATS message.
    #[error("Cannot convert raw NATS message into NATS message")]
    FromRawMessage(#[source] async_nats::Error),

    /// Event cannot be converted into bytes.
    #[error("Cannot convert event to bytes")]
    EvtsIntoBytes(#[source] Box<dyn StdError + Send + Sync + 'static>),

    /// Bytes cannot be converted to event.
    #[error("Cannot convert bytes to event")]
    EvtsFromBytes(#[source] Box<dyn StdError + Send + Sync + 'static>),

    /// Snapshot cannot be encoded as Protocol Buffers.
    #[error("Cannot encode snapshot as Protocol Buffers")]
    EncodeSnapshot(#[from] EncodeError),

    /// Snapshot cannot be decoded from Protocol Buffers.
    #[error("Cannot decode snapshot from Protocol Buffers")]
    DecodeSnapshot(#[from] DecodeError),

    /// A NATS KV bucket cannot be obtained.
    #[error("Cannot get NATS KV bucket")]
    GetBucket(#[source] async_nats::Error),

    /// A NATS KV bucket cannot be created.
    #[error("Cannot create NATS KV bucket")]
    CreateBucket(#[source] async_nats::Error),

    /// A snapshot cannot be stored in a NATS KV bucket.
    #[error("Cannot store snapshot in NATS KV bucket")]
    SaveSnapshot(#[source] async_nats::Error),

    /// A snapshot cannot be loaded from a NATS KV bucket.
    #[error("Cannot load snapshot from NATS KV bucket")]
    LoadSnapshot(#[source] async_nats::Error),

    /// Invalid sequence number.
    #[error("Invalid sequence number")]
    InvalidSeqNo(#[source] TrySeqNoFromZero),
}

#[cfg(test)]
pub mod tests {
    pub const NATS_VERSION: &str = "2.9.11";
}
