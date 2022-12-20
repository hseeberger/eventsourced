//! A [SnapshotStore] implementation based on [NATS](https://nats.io/).

use super::{Snapshot, SnapshotStore};
use crate::convert::{TryFromBytes, TryIntoBytes};
use async_nats::{
    connect,
    jetstream::{self, kv::Store, Context as Jetstream},
};
use bytes::{Bytes, BytesMut};
use prost::{DecodeError, EncodeError, Message};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug, Formatter};
use thiserror::Error;
use tracing::debug;
use uuid::Uuid;

/// A [SnapshotStore] implementation based on [NATS](https://nats.io/).
#[derive(Clone)]
pub struct NatsSnapshotStore {
    jetstream: Jetstream,
    bucket: String,
}

impl NatsSnapshotStore {
    #[allow(missing_docs)]
    pub async fn new(config: Config) -> Result<Self, Error> {
        debug!(?config, "Creating NatsSnapshotStore");

        let server_addr = config.server_addr;
        let client = connect(&server_addr).await?;
        let jetstream = jetstream::new(client);

        Ok(Self {
            jetstream,
            bucket: config.bucket,
        })
    }

    async fn get_bucket(&self, name: &str) -> Result<Store, Error> {
        self.jetstream
            .get_key_value(name)
            .await
            .map_err(Error::GetBucket)
    }
}

impl Debug for NatsSnapshotStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("NatsSnapshotStore")
            .field("bucket", &self.bucket)
            .finish()
    }
}

impl SnapshotStore for NatsSnapshotStore {
    type Error = Error;

    async fn save<'a, 'b, S>(
        &'a mut self,
        id: Uuid,
        seq_no: u64,
        state: &'b S,
    ) -> Result<(), Self::Error>
    where
        'b: 'a,
        S: TryIntoBytes + Send + Sync + 'a,
    {
        let mut bytes = BytesMut::new();
        let state = state
            .try_into_bytes()
            .map_err(|source| Error::EvtsIntoBytes(Box::new(source)))?;
        let snapshot = proto::Snapshot { seq_no, state };
        snapshot.encode(&mut bytes)?;

        self.get_bucket(&self.bucket)
            .await?
            .put(id.to_string(), bytes.into())
            .await
            .map_err(Error::SaveSnapshot)?;
        debug!(%id, %seq_no, "Saved snapshot");

        Ok(())
    }

    async fn load<S>(&self, id: Uuid) -> Result<Option<Snapshot<S>>, Self::Error>
    where
        S: TryFromBytes,
    {
        let snapshot = self
            .get_bucket(&self.bucket)
            .await?
            .get(id.to_string())
            .await
            .map_err(Error::LoadSnapshot)?
            .map(|bytes| {
                proto::Snapshot::decode(Bytes::from(bytes))
                    .map_err(Error::DecodeEvts)
                    .and_then(|proto::Snapshot { seq_no, state }| {
                        S::try_from_bytes(state)
                            .map_err(|source| Error::EvtsFromBytes(Box::new(source)))
                            .map(|state| Snapshot { seq_no, state })
                    })
            })
            .transpose()?;

        if snapshot.is_some() {
            debug!(%id, "Loaded snapshot");
        } else {
            debug!(%id, "No snapshot to load");
        }

        Ok(snapshot)
    }
}

/// Configuration for the [SnapshotStore].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server_addr: String,
    pub bucket: String,
}

impl Config {
    #[allow(missing_docs)]
    pub fn new<S, T>(server_addr: S, bucket_prefix: T) -> Self
    where
        S: Into<String>,
        T: Into<String>,
    {
        Self {
            server_addr: server_addr.into(),
            bucket: bucket_prefix.into(),
        }
    }
}

impl Default for Config {
    /// Use "localhost:4222" for `server_addr` and "snapshots" for `bucket`.
    fn default() -> Self {
        Self::new("localhost:4222", "snapshots")
    }
}

/// Errors from the [NatsSnapshotStore].
#[derive(Debug, Error)]
pub enum Error {
    /// The connection to the NATS server cannot be established.
    #[error("Cannot connect to NATS server")]
    Connect(#[from] std::io::Error),

    /// A NATS KV bucket cannot be obtained.
    #[error("Cannot get NATS KV bucket")]
    GetBucket(#[source] async_nats::Error),

    /// Events cannot be converted into bytes.
    #[error("Cannot convert events to bytes")]
    EvtsIntoBytes(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),

    /// Bytes cannot be converted to events.
    #[error("Cannot convert bytes to events")]
    EvtsFromBytes(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),

    /// Events cannot be encoded as Protocol Buffers.
    #[error("Cannot encode events as Protocol Buffers")]
    EncodeEvts(#[from] EncodeError),

    /// Events cannot be decoded from Protocol Buffers.
    #[error("Cannot decode events from Protocol Buffers")]
    DecodeEvts(#[from] DecodeError),

    /// A snapshot cannot be stored in a NATS KV bucket.
    #[error("Cannot store snapshot in NATS KV bucket")]
    SaveSnapshot(#[source] async_nats::Error),

    /// A snapshot cannot be loaded from a NATS KV bucket.
    #[error("Cannot load snapshot from NATS KV bucket")]
    LoadSnapshot(#[source] async_nats::Error),
}

mod proto {
    include!(concat!(env!("OUT_DIR"), "/snapshot_store.nats.rs"));
}
