//! An [EventLog] implementation based on [NATS](https://nats.io/).

use crate::{make_client, AuthConfig, Error};
use async_nats::jetstream::{
    self,
    consumer::{pull, AckPolicy, DeliverPolicy},
    context::Publish,
    stream::{LastRawMessageErrorKind, Stream as JetstreamStream},
    Context as Jetstream, Message,
};
use bytes::Bytes;
use eventsourced::event_log::EventLog;
use futures::{future::ready, Stream, StreamExt, TryStreamExt};
use serde::Deserialize;
use std::{
    error::Error as StdError,
    fmt::{self, Debug, Display, Formatter},
    marker::PhantomData,
    num::NonZeroU64,
    time::Duration,
};
use tracing::{debug, instrument};

/// An [EventLog] implementation based on [NATS](https://nats.io/).
#[derive(Clone)]
pub struct NatsEventLog<I> {
    event_stream_name: String,
    jetstream: Jetstream,
    _id: PhantomData<I>,
}

impl<I> NatsEventLog<I> {
    #[allow(missing_docs)]
    pub async fn new(config: Config) -> Result<Self, Error> {
        debug!(?config, "creating NatsEventLog");

        let client = make_client(config.auth.as_ref(), &config.server_addr).await?;
        let jetstream = jetstream::new(client);

        // Setup stream.
        if config.setup {
            jetstream
                .create_stream(jetstream::stream::Config {
                    name: config.event_stream_name.clone(),
                    subjects: vec![format!("{}.>", config.event_stream_name)],
                    max_bytes: config.event_stream_max_bytes,
                    ..Default::default()
                })
                .await
                .map_err(|error| {
                    Error::Nats(
                        format!("cannot create event stream '{}'", config.event_stream_name),
                        error.into(),
                    )
                })?;
        }

        Ok(Self {
            event_stream_name: config.event_stream_name,
            jetstream,
            _id: PhantomData,
        })
    }

    async fn events<E, F, FromBytes, FromBytesError>(
        &self,
        subject: String,
        seq_no: NonZeroU64,
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
            &self.event_stream_name,
            subject,
            start_at(seq_no),
        )
        .await?;

        Ok(events(msgs, filter, from_bytes).await)
    }
}

impl<I> Debug for NatsEventLog<I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("NatsEventLog")
            .field("stream_name", &self.event_stream_name)
            .finish()
    }
}

impl<I> EventLog for NatsEventLog<I>
where
    I: Debug + Display + Clone + Send + Sync + 'static,
{
    type Id = I;

    type Error = Error;

    #[instrument(skip(self, event, to_bytes))]
    async fn persist<E, ToBytes, ToBytesError>(
        &mut self,
        type_name: &'static str,
        id: &Self::Id,
        last_seq_no: Option<NonZeroU64>,
        event: &E,
        to_bytes: &ToBytes,
    ) -> Result<NonZeroU64, Self::Error>
    where
        ToBytes: Fn(&E) -> Result<Bytes, ToBytesError> + Sync,
        ToBytesError: StdError + Send + Sync + 'static,
    {
        let bytes = to_bytes(event).map_err(|error| Error::ToBytes(error.into()))?;
        let publish = Publish::build().payload(bytes);
        let publish = last_seq_no.into_iter().fold(publish, |p, last_seq_no| {
            p.expected_last_subject_sequence(last_seq_no.get())
        });

        let subject = format!("{}.{type_name}.{id}", self.event_stream_name);
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
        type_name: &'static str,
        id: &Self::Id,
    ) -> Result<Option<NonZeroU64>, Self::Error> {
        let subject = format!("{}.{type_name}.{id}", self.event_stream_name);
        stream(&self.jetstream, &self.event_stream_name)
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
                                self.event_stream_name
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
    async fn events_by_id<E, FromBytes, FromBytesError>(
        &self,
        type_name: &'static str,
        id: &Self::Id,
        seq_no: NonZeroU64,
        from_bytes: FromBytes,
    ) -> Result<impl Stream<Item = Result<(NonZeroU64, E), Self::Error>> + Send, Self::Error>
    where
        E: Send,
        FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Copy + Send + Sync + 'static,
        FromBytesError: StdError + Send + Sync + 'static,
    {
        debug!(
            type_name,
            %id,
            seq_no,
            "building events by ID stream"
        );
        let subject = format!("{}.{}.{id}", self.event_stream_name, type_name);
        self.events(subject, seq_no, |_| true, from_bytes).await
    }

    #[instrument(skip(self, from_bytes))]
    async fn events_by_type<E, FromBytes, FromBytesError>(
        &self,
        type_name: &'static str,
        seq_no: NonZeroU64,
        from_bytes: FromBytes,
    ) -> Result<impl Stream<Item = Result<(NonZeroU64, E), Self::Error>> + Send, Self::Error>
    where
        E: Send,
        FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Copy + Send + Sync + 'static,
        FromBytesError: StdError + Send + Sync + 'static,
    {
        debug!(type_name, seq_no, "building events by type stream");
        let subject = format!("{}.{}.*", self.event_stream_name, type_name);
        self.events(subject, seq_no, |_| true, from_bytes).await
    }
}

/// Configuration for the [NatsEvtLog].
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub server_addr: String,

    pub auth: Option<AuthConfig>,

    #[serde(default = "event_stream_name_default")]
    pub event_stream_name: String,

    #[serde(default = "event_stream_max_bytes_default")]
    pub event_stream_max_bytes: i64,

    #[serde(default)]
    pub setup: bool,
}

impl Default for Config {
    /// The default values used are:
    fn default() -> Self {
        Self {
            server_addr: "localhost:4222".into(),
            auth: None,
            event_stream_name: event_stream_name_default(),
            event_stream_max_bytes: event_stream_max_bytes_default(),
            setup: false,
        }
    }
}

async fn events<E, F, FromBytes, FromBytesError>(
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
        let event = match msg {
            Ok(msg) if filter(&msg) => {
                let event = seq_no(&msg).and_then(|seq_no| {
                    from_bytes(msg.message.payload)
                        .map_err(|error| Error::FromBytes(error.into()))
                        .map(|event| (seq_no, event))
                });
                Some(event)
            }

            Ok(_) => None,

            Err(err) => Some(Err(err)),
        };
        ready(event)
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

fn start_at(seq_no: NonZeroU64) -> DeliverPolicy {
    DeliverPolicy::ByStartSequence {
        start_sequence: seq_no.get(),
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

fn event_stream_name_default() -> String {
    "events".to_string()
}

fn event_stream_max_bytes_default() -> i64 {
    -1
}

#[cfg(test)]
mod tests {
    use crate::{tests::NATS_VERSION, AuthConfig, NatsEventLog, NatsEventLogConfig};
    use error_ext::BoxError;
    use eventsourced::{binarize, event_log::EventLog};
    use futures::{StreamExt, TryStreamExt};
    use std::{future, num::NonZeroU64};
    use testcontainers::{clients::Cli, core::WaitFor};
    use testcontainers_modules::testcontainers::GenericImage;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_event_log() -> Result<(), BoxError> {
        let client = Cli::default();
        let nats_image = GenericImage::new("nats", NATS_VERSION)
            .with_wait_for(WaitFor::message_on_stderr("Server is ready"));
        let container = client.run((
            nats_image,
            vec![
                "-js".to_string(),
                "--user".to_string(),
                "test".to_string(),
                "--pass".to_string(),
                "test".to_string(),
            ],
        ));
        let server_addr = format!("localhost:{}", container.get_host_port_ipv4(4222));

        let config = NatsEventLogConfig {
            server_addr,
            setup: true,
            auth: Some(AuthConfig::UserPassword {
                user: "test".to_string(),
                password: "test".to_string().into(),
            }),
            ..Default::default()
        };
        let mut event_log = NatsEventLog::<Uuid>::new(config).await?;

        let id = Uuid::now_v7();

        // Start testing.

        let last_seq_no = event_log.last_seq_no("counter", &id).await?;
        assert_eq!(last_seq_no, None);

        let last_seq_no = event_log
            .persist("counter", &id, None, &1, &binarize::serde_json::to_bytes)
            .await?;
        assert!(last_seq_no.get() == 1);

        event_log
            .persist(
                "counter",
                &id,
                Some(last_seq_no),
                &2,
                &binarize::serde_json::to_bytes,
            )
            .await?;

        let result = event_log
            .persist(
                "counter",
                &id,
                Some(last_seq_no),
                &3,
                &binarize::serde_json::to_bytes,
            )
            .await;
        assert!(result.is_err());

        event_log
            .persist(
                "counter",
                &id,
                Some(last_seq_no.checked_add(1).expect("overflow")),
                &3,
                &binarize::serde_json::to_bytes,
            )
            .await?;

        let last_seq_no = event_log.last_seq_no("counter", &id).await?;
        assert_eq!(last_seq_no, Some(3.try_into()?));

        let events = event_log
            .events_by_id::<u32, _, _>(
                "counter",
                &id,
                2.try_into()?,
                binarize::serde_json::from_bytes,
            )
            .await?;
        let sum = events
            .take(2)
            .try_fold(0u32, |acc, (_, n)| future::ready(Ok(acc + n)))
            .await?;
        assert_eq!(sum, 5);

        let events = event_log
            .events_by_type::<u32, _, _>(
                "counter",
                NonZeroU64::MIN,
                binarize::serde_json::from_bytes,
            )
            .await?;

        let last_seq_no = event_log
            .clone()
            .persist(
                "counter",
                &id,
                last_seq_no,
                &4,
                &binarize::serde_json::to_bytes,
            )
            .await?;
        event_log
            .clone()
            .persist(
                "counter",
                &id,
                Some(last_seq_no),
                &5,
                &binarize::serde_json::to_bytes,
            )
            .await?;
        let last_seq_no = event_log.last_seq_no("counter", &id).await?;
        assert_eq!(last_seq_no, Some(5.try_into()?));

        let sum = events
            .take(5)
            .try_fold(0u32, |acc, (_, n)| future::ready(Ok(acc + n)))
            .await?;
        assert_eq!(sum, 15);

        Ok(())
    }
}
