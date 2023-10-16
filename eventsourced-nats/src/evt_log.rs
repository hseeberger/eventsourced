//! An [EvtLog] implementation based on [NATS](https://nats.io/).

use crate::Error;
use async_nats::{
    connect,
    jetstream::{
        self,
        consumer::{pull, pull::Stream as MsgStream, AckPolicy, DeliverPolicy},
        context::Publish,
        stream::{LastRawMessageErrorKind, Stream as JetstreamStream},
        Context as Jetstream,
    },
};
use bytes::Bytes;
use eventsourced::{EvtLog, SeqNo};
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    error::Error as StdError,
    fmt::{self, Debug, Formatter},
    num::NonZeroUsize,
    time::Duration,
};

use tracing::debug;
use uuid::Uuid;

/// An [EvtLog] implementation based on [NATS](https://nats.io/).
#[derive(Clone)]
pub struct NatsEvtLog {
    stream_name: String,
    jetstream: Jetstream,
}

impl NatsEvtLog {
    #[allow(missing_docs)]
    pub async fn new(config: Config) -> Result<Self, Error> {
        debug!(?config, "Creating NatsEvtLog");

        let server_addr = config.server_addr;
        let client = connect(&server_addr).await.map_err(|error| {
            Error::Nats(
                format!("cannot connect to NATS server at {server_addr})"),
                error.into(),
            )
        })?;
        let jetstream = jetstream::new(client);

        // Setup stream.
        if config.setup {
            let _ = jetstream
                .create_stream(jetstream::stream::Config {
                    name: "evts".to_string(),
                    subjects: vec!["evts.>".to_string()],
                    ..Default::default()
                })
                .await
                .map_err(|error| {
                    Error::Nats("cannot create NATS 'evt' stream".into(), error.into())
                })?;
        }

        Ok(Self {
            stream_name: config.stream_name,
            jetstream,
        })
    }

    async fn get_stream(&self) -> Result<JetstreamStream, Error> {
        self.jetstream
            .get_stream(&self.stream_name)
            .await
            .map_err(|error| {
                Error::Nats(
                    format!("cannot get NATS stream '{}'", self.stream_name),
                    error.into(),
                )
            })
    }

    async fn get_msgs(
        &self,
        subject: String,
        deliver_policy: DeliverPolicy,
    ) -> Result<MsgStream, Error> {
        self.get_stream()
            .await?
            .create_consumer(pull::Config {
                filter_subject: subject,
                ack_policy: AckPolicy::None, // Important!
                deliver_policy,
                ..Default::default()
            })
            .await
            .map_err(|error| Error::Nats("cannot create NATS consumer".into(), error.into()))?
            .stream()
            .heartbeat(Duration::ZERO) // Important! Even if I cannot remember why :-(
            .messages()
            .await
            .map_err(|error| {
                Error::Nats(
                    "cannot get message stream from NATS consumer".into(),
                    error.into(),
                )
            })
    }
}

impl Debug for NatsEvtLog {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("NatsEvtLog")
            .field("stream_name", &self.stream_name)
            .finish()
    }
}

impl EvtLog for NatsEvtLog {
    type Error = Error;

    async fn persist<E, ToBytes, ToBytesError>(
        &mut self,
        evt: &E,
        tag: Option<String>,
        id: Uuid,
        to_bytes: &ToBytes,
    ) -> Result<SeqNo, Self::Error>
    where
        E: Sync,
        ToBytes: Fn(&E) -> Result<Bytes, ToBytesError> + Sync,
        ToBytesError: StdError + Send + Sync + 'static,
    {
        // Convert event into bytes.
        let bytes = to_bytes(evt).map_err(|error| Error::EvtsIntoBytes(error.into()))?;

        // Publish event to NATS subject and await ACK.
        let subject = match tag {
            Some(tag) => format!("{}.{id}.{tag}", self.stream_name),
            None => format!("{}.{id}.NOTAG", self.stream_name),
        };
        let publish = Publish::build().payload(bytes);
        self.jetstream
            .send_publish(subject, publish)
            .await
            .map_err(|error| Error::Nats("cannot publish events".into(), error.into()))?
            .await
            .map_err(|error| {
                Error::Nats("cannot get ACK for published events".into(), error.into())
            })
            .and_then(|ack| ack.sequence.try_into().map_err(Error::InvalidSeqNo))
    }

    async fn last_seq_no(&self, id: Uuid) -> Result<Option<SeqNo>, Self::Error> {
        let subject = format!("{}.{id}.*", self.stream_name);
        self.get_stream()
            .await?
            .get_last_raw_message_by_subject(&subject)
            .await
            .map_or_else(
                |error| {
                    if error.kind() == LastRawMessageErrorKind::NoMessageFound {
                        debug!(%id, "no last message found");
                        Ok(None)
                    } else {
                        Err(Error::Nats(
                            "cannot get last message for NATS stream".into(),
                            error.into(),
                        ))
                    }
                },
                |msg| Some(msg.sequence.try_into().map_err(Error::InvalidSeqNo)).transpose(),
            )
    }

    async fn evts_by_id<E, FromBytes, FromBytesError>(
        &self,
        id: Uuid,
        from_seq_no: SeqNo,
        from_bytes: FromBytes,
    ) -> Result<impl Stream<Item = Result<(SeqNo, E), Self::Error>> + Send, Self::Error>
    where
        E: Send,
        FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Copy + Send,
        FromBytesError: StdError + Send + Sync + 'static,
    {
        debug!(%id, %from_seq_no, "building events by ID stream");

        // Get message stream.
        let subject = format!("{}.{id}.*", self.stream_name);
        let deliver_policy = DeliverPolicy::ByStartSequence {
            start_sequence: from_seq_no.as_u64(),
        };
        let msgs = self.get_msgs(subject, deliver_policy).await?;

        // Transform message stream into event stream.
        let evts = msgs.map(move |msg| {
            let msg = msg.map_err(|error| {
                Error::Nats(
                    "cannot get message from NATS message stream".into(),
                    error.into(),
                )
            })?;
            let seq_no: SeqNo = msg
                .info()
                .map_err(|error| Error::Nats("cannot get message info".into(), error))
                .and_then(|info| info.stream_sequence.try_into().map_err(Error::InvalidSeqNo))?;
            from_bytes(msg.message.payload)
                .map_err(|error| Error::EvtsFromBytes(Box::new(error)))
                .map(|evt| (seq_no, evt))
        });

        Ok(evts)
    }

    async fn evts_by_tag<E, FromBytes, FromBytesError>(
        &self,
        tag: String,
        from_seq_no: SeqNo,
        from_bytes: FromBytes,
    ) -> Result<impl Stream<Item = Result<(SeqNo, E), Self::Error>> + Send, Self::Error>
    where
        E: Send,
        FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Copy + Send + Sync + 'static,
        FromBytesError: StdError + Send + Sync + 'static,
    {
        debug!(tag, %from_seq_no, "building events by tag stream");

        // Get message stream.
        let subject = format!("{}.*.{tag}", self.stream_name);
        let deliver_policy = DeliverPolicy::ByStartSequence {
            start_sequence: from_seq_no.as_u64(),
        };
        let msgs = self.get_msgs(subject, deliver_policy).await?;

        // Transform message stream into event stream.
        let evts = msgs.map(move |msg| {
            let msg = msg.map_err(|error| {
                Error::Nats(
                    "cannot get message from NATS message stream".into(),
                    error.into(),
                )
            })?;
            let seq_no: SeqNo = msg
                .info()
                .map_err(|error| Error::Nats("cannot get message info".into(), error))
                .and_then(|info| info.stream_sequence.try_into().map_err(Error::InvalidSeqNo))?;
            from_bytes(msg.message.payload)
                .map_err(|error| Error::EvtsFromBytes(Box::new(error)))
                .map(|evt| (seq_no, evt))
        });

        Ok(evts)
    }
}

/// Configuration for the [NatsEvtLog].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    server_addr: String,
    #[serde(default = "stream_name_default")]
    stream_name: String,
    #[serde(default = "id_broadcast_capacity_default")]
    id_broadcast_capacity: NonZeroUsize,
    #[serde(default)]
    setup: bool,
}

impl Config {
    /// Change the `server_addr`.
    pub fn with_server_addr<T>(self, server_addr: T) -> Self
    where
        T: Into<String>,
    {
        let server_addr = server_addr.into();
        Self {
            server_addr,
            ..self
        }
    }

    /// Change the `stream_name`.
    pub fn with_stream_name<T>(self, stream_name: T) -> Self
    where
        T: Into<String>,
    {
        let stream_name = stream_name.into();
        Self {
            stream_name,
            ..self
        }
    }

    /// Change the `setup` flag.
    pub fn with_setup(self, setup: bool) -> Self {
        Self { setup, ..self }
    }
}

impl Default for Config {
    /// Use "localhost:4222" for `server_addr` and "evts" for `stream_name`.
    fn default() -> Self {
        Self {
            server_addr: "localhost:4222".into(),
            stream_name: "evts".into(),
            id_broadcast_capacity: id_broadcast_capacity_default(),
            setup: false,
        }
    }
}

fn stream_name_default() -> String {
    "evts".to_string()
}

const fn id_broadcast_capacity_default() -> NonZeroUsize {
    NonZeroUsize::MIN
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::NATS_VERSION;
    use eventsourced::convert;
    use futures::TryStreamExt;
    use std::future;
    use testcontainers::{clients::Cli, core::WaitFor};
    use testcontainers_modules::testcontainers::GenericImage;

    #[tokio::test]
    async fn test_evt_log() -> Result<(), Box<dyn StdError + Send + Sync>> {
        let client = Cli::default();
        let nats_image = GenericImage::new("nats", NATS_VERSION)
            .with_wait_for(WaitFor::message_on_stderr("Server is ready"));
        let container = client.run((nats_image, vec!["-js".to_string()]));
        let server_addr = format!("localhost:{}", container.get_host_port_ipv4(4222));

        let config = Config::default()
            .with_server_addr(server_addr)
            .with_setup(true);
        let mut evt_log = NatsEvtLog::new(config).await?;

        let id = Uuid::now_v7();

        // Start testing.

        let last_seq_no = evt_log.last_seq_no(id).await?;
        assert_eq!(last_seq_no, None);

        evt_log
            .persist(&1, Some("tag".to_string()), id, &convert::prost::to_bytes)
            .await?;
        evt_log
            .persist(&2, None, id, &convert::prost::to_bytes)
            .await?;
        evt_log
            .persist(&3, Some("tag".to_string()), id, &convert::prost::to_bytes)
            .await?;
        let last_seq_no = evt_log.last_seq_no(id).await?;
        assert_eq!(last_seq_no, Some(3.try_into().unwrap()));

        let evts = evt_log
            .evts_by_id::<i32, _, _>(id, 2.try_into().unwrap(), convert::prost::from_bytes)
            .await?;
        let sum = evts
            .take(2)
            .try_fold(0i32, |acc, (_, n)| future::ready(Ok(acc + n)))
            .await?;
        assert_eq!(sum, 5);

        let evts_by_tag = evt_log
            .evts_by_tag::<i32, _, _>("tag".to_string(), SeqNo::MIN, convert::prost::from_bytes)
            .await?;
        let sum = evts_by_tag
            .take(2)
            .try_fold(0i32, |acc, (_, n)| future::ready(Ok(acc + n)))
            .await?;
        assert_eq!(sum, 4);

        let evts = evt_log
            .evts_by_id::<i32, _, _>(id, 1.try_into().unwrap(), convert::prost::from_bytes)
            .await?;

        let evts_by_tag = evt_log
            .evts_by_tag::<i32, _, _>("tag".to_string(), SeqNo::MIN, convert::prost::from_bytes)
            .await?;

        evt_log
            .clone()
            .persist(&4, None, id, &convert::prost::to_bytes)
            .await?;
        evt_log
            .clone()
            .persist(&5, Some("tag".to_string()), id, &convert::prost::to_bytes)
            .await?;
        let last_seq_no = evt_log.last_seq_no(id).await?;
        assert_eq!(last_seq_no, Some(5.try_into().unwrap()));

        let sum = evts
            .take(5)
            .try_fold(0i32, |acc, (_, n)| future::ready(Ok(acc + n)))
            .await?;
        assert_eq!(sum, 15);

        let sum = evts_by_tag
            .take(3)
            .try_fold(0i32, |acc, (_, n)| future::ready(Ok(acc + n)))
            .await?;
        assert_eq!(sum, 9);
        Ok(())
    }
}
