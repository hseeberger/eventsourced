//! An [EvtLog] implementation based on [NATS](https://nats.io/).

use crate::Error;
use async_nats::{
    connect,
    jetstream::{
        self,
        consumer::{pull, AckPolicy, DeliverPolicy},
        context::Publish,
        stream::{LastRawMessageErrorKind, Stream as JetstreamStream},
        Context as Jetstream, Message,
    },
};
use bytes::Bytes;
use eventsourced::{EvtLog, SeqNo};
use futures::{future::ready, Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::{
    error::Error as StdError,
    fmt::{self, Debug, Formatter},
    time::Duration,
};
use tracing::debug;
use uuid::Uuid;

const TAG: &str = "EventSourced-Tag";

/// An [EvtLog] implementation based on [NATS](https://nats.io/).
#[derive(Clone)]
pub struct NatsEvtLog {
    evt_stream_name: String,
    jetstream: Jetstream,
}

impl NatsEvtLog {
    #[allow(missing_docs)]
    pub async fn new(config: Config) -> Result<Self, Error> {
        debug!(?config, "creating NatsEvtLog");

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
            jetstream
                .create_stream(jetstream::stream::Config {
                    name: config.evt_stream_name.clone(),
                    subjects: vec![format!("{}.>", config.evt_stream_name)],
                    ..Default::default()
                })
                .await
                .map_err(|error| {
                    Error::Nats(
                        format!("cannot create tag stream '{}'", config.evt_stream_name),
                        error.into(),
                    )
                })?;
        }

        Ok(Self {
            evt_stream_name: config.evt_stream_name,
            jetstream,
        })
    }

    async fn evts<E, F, FromBytes, FromBytesError>(
        &self,
        subject: String,
        from: SeqNo,
        filter: F,
        from_bytes: FromBytes,
    ) -> Result<impl Stream<Item = Result<(SeqNo, E), Error>> + Send, Error>
    where
        E: Send,
        F: Fn(&Message) -> bool + Send,
        FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Copy + Send + Sync + 'static,
        FromBytesError: StdError + Send + Sync + 'static,
    {
        let msgs = msgs(
            &self.jetstream,
            &self.evt_stream_name,
            subject,
            from_seq_no_policy(from),
        )
        .await?;

        Ok(evts(msgs, filter, from_bytes).await)
    }
}

impl Debug for NatsEvtLog {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("NatsEvtLog")
            .field("stream_name", &self.evt_stream_name)
            .finish()
    }
}

impl EvtLog for NatsEvtLog {
    type Error = Error;

    async fn persist<E, ToBytes, ToBytesError>(
        &mut self,
        evt: &E,
        tag: Option<&str>,
        r#type: &str,
        id: Uuid,
        last_seq_no: Option<SeqNo>,
        to_bytes: &ToBytes,
    ) -> Result<SeqNo, Self::Error>
    where
        E: Sync,
        ToBytes: Fn(&E) -> Result<Bytes, ToBytesError> + Sync,
        ToBytesError: StdError + Send + Sync + 'static,
    {
        let bytes = to_bytes(evt).map_err(|error| Error::IntoBytes(error.into()))?;
        let publish = Publish::build().payload(bytes);
        let publish = tag.into_iter().fold(publish, |p, tag| p.header(TAG, tag));
        let publish = last_seq_no.into_iter().fold(publish, |p, last_seq_no| {
            p.expected_last_subject_sequence(last_seq_no.as_u64())
        });

        let subject = format!("{}.{type}.{id}", self.evt_stream_name);
        self.jetstream
            .send_publish(subject, publish)
            .await
            .map_err(|error| Error::Nats("cannot publish event".into(), error.into()))?
            .await
            .map_err(|error| Error::Nats("cannot get ACK for published event".into(), error.into()))
            .and_then(|ack| ack.sequence.try_into().map_err(Error::InvalidSeqNo))
    }

    async fn last_seq_no(&self, r#type: &str, id: Uuid) -> Result<Option<SeqNo>, Self::Error> {
        let subject = format!("{}.{type}.{id}", self.evt_stream_name);
        stream(&self.jetstream, &self.evt_stream_name)
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
                            format!(
                                "cannot get last message for NATS stream '{}'",
                                self.evt_stream_name
                            ),
                            error.into(),
                        ))
                    }
                },
                |msg| Some(msg.sequence.try_into().map_err(Error::InvalidSeqNo)).transpose(),
            )
    }

    async fn evts_by_id<E, FromBytes, FromBytesError>(
        &self,
        r#type: &str,
        id: Uuid,
        from: SeqNo,
        from_bytes: FromBytes,
    ) -> Result<impl Stream<Item = Result<(SeqNo, E), Self::Error>> + Send, Self::Error>
    where
        E: Send,
        FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Copy + Send + Sync + 'static,
        FromBytesError: StdError + Send + Sync + 'static,
    {
        debug!(%id, %from, "building events by ID stream");
        let subject = format!("{}.{type}.{id}", self.evt_stream_name);
        self.evts(subject, from, |_| true, from_bytes).await
    }

    async fn evts_by_type<E, FromBytes, FromBytesError>(
        &self,
        r#type: &str,
        from: SeqNo,
        from_bytes: FromBytes,
    ) -> Result<impl Stream<Item = Result<(SeqNo, E), Self::Error>> + Send, Self::Error>
    where
        E: Send,
        FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Copy + Send + Sync + 'static,
        FromBytesError: StdError + Send + Sync + 'static,
    {
        debug!(r#type, %from, "building events by type stream");
        let subject = format!("{}.{type}.*", self.evt_stream_name);
        self.evts(subject, from, |_| true, from_bytes).await
    }

    async fn evts_by_tag<E, FromBytes, FromBytesError>(
        &self,
        tag: String,
        from: SeqNo,
        from_bytes: FromBytes,
    ) -> Result<impl Stream<Item = Result<(SeqNo, E), Self::Error>> + Send, Self::Error>
    where
        E: Send,
        FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Copy + Send + Sync + 'static,
        FromBytesError: StdError + Send + Sync + 'static,
    {
        debug!(tag, %from, "building events by tag stream");
        let subject = format!("{}.*.*", self.evt_stream_name);
        self.evts(subject, from, move |msg| has_tag(msg, &tag), from_bytes)
            .await
    }
}

/// Configuration for the [NatsEvtLog].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    server_addr: String,

    #[serde(default = "evt_stream_name_default")]
    evt_stream_name: String,

    #[serde(default = "tag_stream_name_default")]
    tag_stream_name: String,

    #[serde(default)]
    setup: bool,
}

impl Config {
    /// Change the `server_addr`.
    pub fn with_server_addr<T>(self, server_addr: T) -> Self
    where
        T: ToString,
    {
        let server_addr = server_addr.to_string();
        Self {
            server_addr,
            ..self
        }
    }

    /// Change the `stream_name`.
    pub fn with_stream_name<T>(self, stream_name: T) -> Self
    where
        T: ToString,
    {
        let stream_name = stream_name.to_string();
        Self {
            evt_stream_name: stream_name,
            ..self
        }
    }

    /// Change the `setup` flag.
    pub fn with_setup(self, setup: bool) -> Self {
        Self { setup, ..self }
    }
}

impl Default for Config {
    /// The default values used are:
    fn default() -> Self {
        Self {
            server_addr: "localhost:4222".into(),
            evt_stream_name: evt_stream_name_default(),
            tag_stream_name: tag_stream_name_default(),
            setup: false,
        }
    }
}

async fn evts<E, F, FromBytes, FromBytesError>(
    msgs: impl Stream<Item = Result<Message, Error>> + Send,
    filter: F,
    from_bytes: FromBytes,
) -> impl Stream<Item = Result<(SeqNo, E), Error>> + Send
where
    E: Send,
    F: Fn(&Message) -> bool + Send,
    FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Copy + Send + Sync + 'static,
    FromBytesError: StdError + Send + Sync + 'static,
{
    msgs.filter_map(move |msg| {
        let evt = match msg {
            Ok(msg) if filter(&msg) => {
                let evt = seq_no(&msg).and_then(|seq_no| {
                    from_bytes(msg.message.payload)
                        .map_err(|error| Error::FromBytes(error.into()))
                        .map(|evt| (seq_no, evt))
                });
                Some(evt)
            }

            Ok(_) => None,

            Err(err) => Some(Err(err)),
        };
        ready(evt)
    })
}

async fn msgs(
    jetstream: &Jetstream,
    stream_name: &str,
    subject: String,
    deliver_policy: DeliverPolicy,
) -> Result<impl Stream<Item = Result<Message, Error>> + Send, Error> {
    stream(jetstream, stream_name)
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
        .map(|stream| {
            stream.map_err(|error| {
                Error::Nats(
                    "cannot get message from NATS message stream".into(),
                    error.into(),
                )
            })
        })
}

async fn stream(jetstream: &Jetstream, stream_name: &str) -> Result<JetstreamStream, Error> {
    jetstream.get_stream(stream_name).await.map_err(|error| {
        Error::Nats(
            format!("cannot get NATS stream '{stream_name}'"),
            error.into(),
        )
    })
}

fn from_seq_no_policy(from: SeqNo) -> DeliverPolicy {
    DeliverPolicy::ByStartSequence {
        start_sequence: from.as_u64(),
    }
}

fn seq_no(msg: &Message) -> Result<SeqNo, Error> {
    msg.info()
        .map_err(|error| Error::Nats("cannot get message info".into(), error))
        .and_then(|info| info.stream_sequence.try_into().map_err(Error::InvalidSeqNo))
}

fn has_tag(msg: &Message, tag: &str) -> bool {
    msg.headers
        .as_ref()
        .and_then(|headers| headers.get(TAG))
        .map(|value| value.as_str() == tag)
        .unwrap_or_default()
}

fn evt_stream_name_default() -> String {
    "evts".to_string()
}

fn tag_stream_name_default() -> String {
    "tags".to_string()
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

        let last_seq_no = evt_log.last_seq_no("test", id).await?;
        assert_eq!(last_seq_no, None);

        let last_seq_no = evt_log
            .persist(&1, Some("tag"), "test", id, None, &convert::prost::to_bytes)
            .await?;
        assert!(last_seq_no.as_u64() == 1);

        evt_log
            .persist(
                &2,
                None,
                "test",
                id,
                Some(last_seq_no),
                &convert::prost::to_bytes,
            )
            .await?;

        let result = evt_log
            .persist(
                &3,
                Some("tag"),
                "test",
                id,
                Some(last_seq_no),
                &convert::prost::to_bytes,
            )
            .await;
        assert!(result.is_err());

        evt_log
            .persist(
                &3,
                Some("tag"),
                "test",
                id,
                Some(last_seq_no.succ()),
                &convert::prost::to_bytes,
            )
            .await?;

        let last_seq_no = evt_log.last_seq_no("test", id).await?;
        assert_eq!(last_seq_no, Some(3.try_into()?));

        let evts = evt_log
            .evts_by_id::<i32, _, _>("test", id, 2.try_into()?, convert::prost::from_bytes)
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
            .evts_by_id::<i32, _, _>("test", id, 1.try_into()?, convert::prost::from_bytes)
            .await?;

        let evts_by_tag = evt_log
            .evts_by_tag::<i32, _, _>("tag".to_string(), SeqNo::MIN, convert::prost::from_bytes)
            .await?;

        let last_seq_no = evt_log
            .clone()
            .persist(&4, None, "test", id, last_seq_no, &convert::prost::to_bytes)
            .await?;
        evt_log
            .clone()
            .persist(
                &5,
                Some("tag"),
                "test",
                id,
                Some(last_seq_no),
                &convert::prost::to_bytes,
            )
            .await?;
        let last_seq_no = evt_log.last_seq_no("test", id).await?;
        assert_eq!(last_seq_no, Some(5.try_into()?));

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
