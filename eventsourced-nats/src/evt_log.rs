//! An [EvtLog] implementation based on [NATS](https://nats.io/).

use crate::Error;
use async_nats::{
    jetstream::{
        self,
        consumer::{pull, AckPolicy, DeliverPolicy},
        context::Publish,
        stream::{LastRawMessageErrorKind, Stream as JetstreamStream},
        Context as Jetstream, Message,
    },
    ConnectOptions,
};
use bytes::Bytes;
use eventsourced::EvtLog;
use futures::{future::ready, Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::{
    error::Error as StdError,
    fmt::{self, Debug, Display, Formatter},
    marker::PhantomData,
    num::NonZeroU64,
    path::PathBuf,
    time::Duration,
};
use tracing::{debug, instrument};

/// An [EvtLog] implementation based on [NATS](https://nats.io/).
#[derive(Clone)]
pub struct NatsEvtLog<I> {
    evt_stream_name: String,
    jetstream: Jetstream,
    _id: PhantomData<I>,
}

impl<I> NatsEvtLog<I> {
    #[allow(missing_docs)]
    pub async fn new(config: Config) -> Result<Self, Error> {
        debug!(?config, "creating NatsEvtLog");

        let mut options = ConnectOptions::new();
        if let Some(credentials) = config.credentials {
            options = options
                .credentials_file(&credentials)
                .await
                .map_err(|error| {
                    Error::Nats(
                        format!(
                            "cannot read NATS credentials file at {})",
                            credentials.display()
                        ),
                        error.into(),
                    )
                })?;
        };
        let client = options
            .connect(&config.server_addr)
            .await
            .map_err(|error| {
                Error::Nats(
                    format!("cannot connect to NATS server at {})", config.server_addr),
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
                    max_bytes: config.evt_stream_max_bytes,
                    ..Default::default()
                })
                .await
                .map_err(|error| {
                    Error::Nats(
                        format!("cannot create evt stream '{}'", config.evt_stream_name),
                        error.into(),
                    )
                })?;
        }

        Ok(Self {
            evt_stream_name: config.evt_stream_name,
            jetstream,
            _id: PhantomData,
        })
    }

    async fn evts<E, F, FromBytes, FromBytesError>(
        &self,
        subject: String,
        after_seq_no: Option<NonZeroU64>,
        filter: F,
        from_bytes: FromBytes,
    ) -> Result<impl Stream<Item = Result<(NonZeroU64, E), Error>> + Send, Error>
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
            policy(after_seq_no),
        )
        .await?;

        Ok(evts(msgs, filter, from_bytes).await)
    }
}

impl<I> Debug for NatsEvtLog<I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("NatsEvtLog")
            .field("stream_name", &self.evt_stream_name)
            .finish()
    }
}

impl<I> EvtLog for NatsEvtLog<I>
where
    I: Debug + Display + Clone + Send + Sync + 'static,
{
    type Id = I;

    type Error = Error;

    #[instrument(skip(self, to_bytes))]
    async fn persist<E, ToBytes, ToBytesError>(
        &mut self,
        evt: &E,
        type_name: &str,
        id: &Self::Id,
        last_seq_no: Option<NonZeroU64>,
        to_bytes: &ToBytes,
    ) -> Result<NonZeroU64, Self::Error>
    where
        E: Debug + Sync,
        ToBytes: Fn(&E) -> Result<Bytes, ToBytesError> + Sync,
        ToBytesError: StdError + Send + Sync + 'static,
    {
        let bytes = to_bytes(evt).map_err(|error| Error::IntoBytes(error.into()))?;
        let publish = Publish::build().payload(bytes);
        let publish = last_seq_no.into_iter().fold(publish, |p, last_seq_no| {
            p.expected_last_subject_sequence(last_seq_no.get())
        });

        let subject = format!("{}.{type_name}.{id}", self.evt_stream_name);
        self.jetstream
            .send_publish(subject, publish)
            .await
            .map_err(|error| Error::Nats("cannot publish event".into(), error.into()))?
            .await
            .map_err(|error| Error::Nats("cannot get ACK for published event".into(), error.into()))
            .and_then(|ack| {
                ack.sequence
                    .try_into()
                    .map_err(|_| Error::InvalidNonZeroU64)
            })
    }

    #[instrument(skip(self))]
    async fn last_seq_no(
        &self,
        type_name: &str,
        id: &Self::Id,
    ) -> Result<Option<NonZeroU64>, Self::Error> {
        let subject = format!("{}.{type_name}.{id}", self.evt_stream_name);
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
                |msg| {
                    Some(
                        msg.sequence
                            .try_into()
                            .map_err(|_| Error::InvalidNonZeroU64),
                    )
                    .transpose()
                },
            )
    }

    #[instrument(skip(self, from_bytes))]
    async fn evts_by_id<E, FromBytes, FromBytesError>(
        &self,
        type_name: &str,
        id: &Self::Id,
        after_seq_no: Option<NonZeroU64>,
        from_bytes: FromBytes,
    ) -> Result<impl Stream<Item = Result<(NonZeroU64, E), Self::Error>> + Send, Self::Error>
    where
        E: Send,
        FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Copy + Send + Sync + 'static,
        FromBytesError: StdError + Send + Sync + 'static,
    {
        debug!(%id, ?after_seq_no, "building events by ID stream");
        let subject = format!("{}.{type_name}.{id}", self.evt_stream_name);
        self.evts(subject, after_seq_no, |_| true, from_bytes).await
    }

    #[instrument(skip(self, from_bytes))]
    async fn evts_by_type<E, FromBytes, FromBytesError>(
        &self,
        type_name: &str,
        after_seq_no: Option<NonZeroU64>,
        from_bytes: FromBytes,
    ) -> Result<impl Stream<Item = Result<(NonZeroU64, E), Self::Error>> + Send, Self::Error>
    where
        E: Send,
        FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Copy + Send + Sync + 'static,
        FromBytesError: StdError + Send + Sync + 'static,
    {
        debug!(type_name, ?after_seq_no, "building events by type stream");
        let subject = format!("{}.{type_name}.*", self.evt_stream_name);
        self.evts(subject, after_seq_no, |_| true, from_bytes).await
    }
}

/// Configuration for the [NatsEvtLog].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub server_addr: String,

    pub credentials: Option<PathBuf>,

    #[serde(default = "evt_stream_name_default")]
    pub evt_stream_name: String,

    #[serde(default = "evt_stream_max_bytes_default")]
    pub evt_stream_max_bytes: i64,

    #[serde(default)]
    pub setup: bool,
}

impl Default for Config {
    /// The default values used are:
    fn default() -> Self {
        Self {
            server_addr: "localhost:4222".into(),
            credentials: None,
            evt_stream_name: evt_stream_name_default(),
            evt_stream_max_bytes: evt_stream_max_bytes_default(),
            setup: false,
        }
    }
}

async fn evts<E, F, FromBytes, FromBytesError>(
    msgs: impl Stream<Item = Result<Message, Error>> + Send,
    filter: F,
    from_bytes: FromBytes,
) -> impl Stream<Item = Result<(NonZeroU64, E), Error>> + Send
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

fn policy(last_seq_no: Option<NonZeroU64>) -> DeliverPolicy {
    match last_seq_no {
        Some(last_seq_no) => DeliverPolicy::ByStartSequence {
            start_sequence: last_seq_no.get() + 1,
        },

        None => DeliverPolicy::All,
    }
}

fn seq_no(msg: &Message) -> Result<NonZeroU64, Error> {
    msg.info()
        .map_err(|error| Error::Nats("cannot get message info".into(), error))
        .and_then(|info| {
            info.stream_sequence
                .try_into()
                .map_err(|_| Error::InvalidNonZeroU64)
        })
}

fn evt_stream_name_default() -> String {
    "evts".to_string()
}

fn evt_stream_max_bytes_default() -> i64 {
    -1
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
    use uuid::Uuid;

    #[tokio::test]
    async fn test_evt_log() -> Result<(), Box<dyn StdError + Send + Sync>> {
        let client = Cli::default();
        let nats_image = GenericImage::new("nats", NATS_VERSION)
            .with_wait_for(WaitFor::message_on_stderr("Server is ready"));
        let container = client.run((nats_image, vec!["-js".to_string()]));
        let server_addr = format!("localhost:{}", container.get_host_port_ipv4(4222));

        let config = Config {
            server_addr,
            setup: true,
            ..Default::default()
        };
        let mut evt_log = NatsEvtLog::<Uuid>::new(config).await?;

        let id = Uuid::now_v7();

        // Start testing.

        let last_seq_no = evt_log.last_seq_no("test", &id).await?;
        assert_eq!(last_seq_no, None);

        let last_seq_no = evt_log
            .persist(&1, "test", &id, None, &convert::prost::to_bytes)
            .await?;
        assert!(last_seq_no.get() == 1);

        evt_log
            .persist(
                &2,
                "test",
                &id,
                Some(last_seq_no),
                &convert::prost::to_bytes,
            )
            .await?;

        let result = evt_log
            .persist(
                &3,
                "test",
                &id,
                Some(last_seq_no),
                &convert::prost::to_bytes,
            )
            .await;
        assert!(result.is_err());

        evt_log
            .persist(
                &3,
                "test",
                &id,
                Some(last_seq_no.checked_add(1).expect("overflow")),
                &convert::prost::to_bytes,
            )
            .await?;

        let last_seq_no = evt_log.last_seq_no("test", &id).await?;
        assert_eq!(last_seq_no, Some(3.try_into()?));

        let evts = evt_log
            .evts_by_id::<i32, _, _>("test", &id, Some(1.try_into()?), convert::prost::from_bytes)
            .await?;
        let sum = evts
            .take(2)
            .try_fold(0i32, |acc, (_, n)| future::ready(Ok(acc + n)))
            .await?;
        assert_eq!(sum, 5);

        let evts = evt_log
            .evts_by_type::<i32, _, _>("test", None, convert::prost::from_bytes)
            .await?;

        let last_seq_no = evt_log
            .clone()
            .persist(&4, "test", &id, last_seq_no, &convert::prost::to_bytes)
            .await?;
        evt_log
            .clone()
            .persist(
                &5,
                "test",
                &id,
                Some(last_seq_no),
                &convert::prost::to_bytes,
            )
            .await?;
        let last_seq_no = evt_log.last_seq_no("test", &id).await?;
        assert_eq!(last_seq_no, Some(5.try_into()?));

        let sum = evts
            .take(5)
            .try_fold(0i32, |acc, (_, n)| future::ready(Ok(acc + n)))
            .await?;
        assert_eq!(sum, 15);

        Ok(())
    }
}
