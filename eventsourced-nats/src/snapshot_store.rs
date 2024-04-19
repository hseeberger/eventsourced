//! A [SnapshotStore] implementation based on [NATS](https://nats.io/).

use crate::{make_client, AuthConfig, Error};
use async_nats::jetstream::{self, kv::Store, Context as Jetstream};
use bytes::{Bytes, BytesMut};
use eventsourced::snapshot_store::{Snapshot, SnapshotStore};
use prost::Message;
use serde::Deserialize;
use std::{
    error::Error as StdError,
    fmt::{self, Debug, Display, Formatter},
    marker::PhantomData,
    num::NonZeroU64,
};
use tracing::debug;

/// A [SnapshotStore] implementation based on [NATS](https://nats.io/).
#[derive(Clone)]
pub struct NatsSnapshotStore<I> {
    jetstream: Jetstream,
    bucket: String,
    _id: PhantomData<I>,
}

impl<I> NatsSnapshotStore<I> {
    #[allow(missing_docs)]
    pub async fn new(config: Config) -> Result<Self, Error> {
        debug!(?config, "creating NatsSnapshotStore");

        let client = make_client(config.auth.as_ref(), &config.server_addr).await?;
        let jetstream = jetstream::new(client);

        // Setup bucket.
        if config.setup {
            let _ = jetstream
                .create_key_value(jetstream::kv::Config {
                    bucket: config.bucket_name.clone(),
                    max_bytes: config.bucket_max_bytes,
                    ..Default::default()
                })
                .await
                .map_err(|error| {
                    Error::Nats("cannot create NATS KV bucket".into(), error.into())
                })?;
        }

        Ok(Self {
            jetstream,
            bucket: config.bucket_name,
            _id: PhantomData,
        })
    }

    async fn get_bucket(&self, name: &str) -> Result<Store, Error> {
        self.jetstream
            .get_key_value(name)
            .await
            .map_err(|error| Error::Nats("cannot get NATS KV bucket".into(), error.into()))
    }
}

impl<I> Debug for NatsSnapshotStore<I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("NatsSnapshotStore")
            .field("bucket", &self.bucket)
            .finish()
    }
}

impl<I> SnapshotStore for NatsSnapshotStore<I>
where
    I: Debug + Display + Clone + Send + Sync + 'static,
{
    type Id = I;

    type Error = Error;

    async fn save<S, ToBytes, ToBytesError>(
        &mut self,
        id: &Self::Id,
        seq_no: NonZeroU64,
        state: &S,
        to_bytes: &ToBytes,
    ) -> Result<(), Self::Error>
    where
        S: Send,
        ToBytes: Fn(&S) -> Result<Bytes, ToBytesError> + Sync,
        ToBytesError: StdError + Send + Sync + 'static,
    {
        let mut bytes = BytesMut::new();
        let state = to_bytes(state).map_err(|error| Error::ToBytes(Box::new(error)))?;
        let snapshot = proto::Snapshot {
            seq_no: seq_no.get(),
            state,
        };
        snapshot.encode(&mut bytes)?;

        self.get_bucket(&self.bucket)
            .await?
            .put(id.to_string(), bytes.into())
            .await
            .map_err(|error| {
                Error::Nats(
                    "cannot store snapshot in NATS KV bucket".into(),
                    error.into(),
                )
            })?;
        debug!(%id, %seq_no, "saved snapshot");

        Ok(())
    }

    async fn load<S, FromBytes, FromBytesError>(
        &self,
        id: &Self::Id,
        from_bytes: FromBytes,
    ) -> Result<Option<Snapshot<S>>, Self::Error>
    where
        FromBytes: Fn(Bytes) -> Result<S, FromBytesError> + Send,
        FromBytesError: StdError + Send + Sync + 'static,
    {
        let snapshot = self
            .get_bucket(&self.bucket)
            .await?
            .get(id.to_string())
            .await
            .map_err(|error| {
                Error::Nats(
                    "cannot load snapshot from NATS KV bucket".into(),
                    error.into(),
                )
            })?
            .map(|bytes| {
                proto::Snapshot::decode(bytes)
                    .map_err(Error::DecodeSnapshot)
                    .and_then(|proto::Snapshot { seq_no, state }| {
                        from_bytes(state)
                            .map_err(|error| Error::FromBytes(Box::new(error)))
                            .and_then(|state| {
                                seq_no
                                    .try_into()
                                    .map_err(|_| Error::InvalidNonZeroU64)
                                    .map(|seq_no| Snapshot::new(seq_no, state))
                            })
                    })
            })
            .transpose()?;

        if snapshot.is_some() {
            debug!(%id, "loaded snapshot");
        } else {
            debug!(%id, "no snapshot to load");
        }

        Ok(snapshot)
    }
}

/// Configuration for the [SnapshotStore].
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub server_addr: String,

    pub auth: Option<AuthConfig>,

    #[serde(default = "bucket_name_default")]
    pub bucket_name: String,

    #[serde(default = "bucket_max_bytes_default")]
    pub bucket_max_bytes: i64,

    #[serde(default)]
    pub setup: bool,
}

impl Default for Config {
    /// Use "localhost:4222" for `server_addr` and "snapshots" for `bucket`.
    fn default() -> Self {
        Self {
            server_addr: "localhost:4222".to_string(),
            auth: None,
            bucket_name: bucket_name_default(),
            bucket_max_bytes: bucket_max_bytes_default(),
            setup: false,
        }
    }
}

fn bucket_max_bytes_default() -> i64 {
    -1
}

fn bucket_name_default() -> String {
    "snapshots".to_string()
}

mod proto {
    include!(concat!(env!("OUT_DIR"), "/snapshot_store.rs"));
}

#[cfg(test)]
mod tests {
    use crate::{tests::NATS_VERSION, NatsSnapshotStore, NatsSnapshotStoreConfig};
    use error_ext::BoxError;
    use eventsourced::{binarize, snapshot_store::SnapshotStore};
    use testcontainers::{clients::Cli, core::WaitFor};
    use testcontainers_modules::testcontainers::GenericImage;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_snapshot_store() -> Result<(), BoxError> {
        let client = Cli::default();
        let nats_image = GenericImage::new("nats", NATS_VERSION)
            .with_wait_for(WaitFor::message_on_stderr("Server is ready"));
        let container = client.run((nats_image, vec!["-js".to_string()]));
        let server_addr = format!("localhost:{}", container.get_host_port_ipv4(4222));

        let config = NatsSnapshotStoreConfig {
            server_addr,
            setup: true,
            ..Default::default()
        };
        let mut snapshot_store = NatsSnapshotStore::new(config).await?;

        let id = Uuid::now_v7();

        let snapshot = snapshot_store
            .load::<i32, _, _>(&id, &binarize::serde_json::from_bytes)
            .await?;
        assert!(snapshot.is_none());

        let seq_no = 42.try_into().unwrap();
        let state = 666;

        snapshot_store
            .save(&id, seq_no, &state, &binarize::serde_json::to_bytes)
            .await?;

        let snapshot = snapshot_store
            .load::<i32, _, _>(&id, &binarize::serde_json::from_bytes)
            .await?;

        assert!(snapshot.is_some());
        let snapshot = snapshot.unwrap();
        assert_eq!(snapshot.seq_no, seq_no);
        assert_eq!(snapshot.state, state);

        Ok(())
    }
}
