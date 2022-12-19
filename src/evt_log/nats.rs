//! An [EvtLog] implementation based on [NATS](https://nats.io/).

use super::EvtLog;
use crate::convert::{TryFromBytes, TryIntoBytes};
use async_nats::{
    connect,
    jetstream::{
        self, consumer::pull, response, stream::Stream as JetstreamStream, Context as Jetstream,
    },
    HeaderMap, Message,
};
use async_stream::stream;
use bytes::BytesMut;
use futures::{stream, Stream, StreamExt};
use prost::{DecodeError, EncodeError, Message as ProstMessage};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{self, Debug, Formatter},
    io,
    str::FromStr,
};
use thiserror::Error;
use tracing::debug;
use uuid::Uuid;

const SEQ_NO: &str = "seq_no";
const LEN: &str = "len";

/// An [EvtLog] implementation based on [NATS](https://nats.io/).
#[derive(Clone)]
pub struct NatsEvtLog {
    jetstream: Jetstream,
    stream_name: String,
}

impl Debug for NatsEvtLog {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("NatsEvtLog")
            .field("stream_name", &self.stream_name)
            .finish()
    }
}

impl NatsEvtLog {
    #[allow(missing_docs)]
    pub async fn new(config: Config) -> Result<Self, Error> {
        debug!(?config, "Creating NatsEvtLog");

        let server_addr = config.server_addr;
        let client = connect(&server_addr).await?;
        let jetstream = jetstream::new(client);

        Ok(Self {
            jetstream,
            stream_name: config.stream_name,
        })
    }

    async fn get_stream(&self) -> Result<JetstreamStream, Error> {
        self.jetstream
            .get_stream(&self.stream_name)
            .await
            .map_err(Error::GetStream)
    }
}

impl EvtLog for NatsEvtLog {
    type Error = Error;

    async fn persist<'a, 'b, E>(
        &'a mut self,
        id: Uuid,
        evts: &'b [E],
        seq_no: u64,
    ) -> Result<(), Self::Error>
    where
        'b: 'a,
        E: TryIntoBytes + Send + Sync + 'a,
    {
        if evts.is_empty() {
            debug!(%id, "Not publishing to NATS, because no events given");
            return Ok(());
        }

        // Convert events into bytes.
        let len = evts.len();
        let evts = evts
            .iter()
            .map(|evt| evt.try_into_bytes())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| Error::EvtsIntoBytes(Box::new(source)))?;
        let evts = proto::Evts { evts };
        let mut bytes = BytesMut::new();
        evts.encode(&mut bytes)?;

        // Determine NATS subject.
        let stream_name = &self.stream_name;
        let subject = format!("{stream_name}.{id}");
        debug!(%id, ?subject, "Publishing to NATS");

        // Publish events to NATS subject and await ACK.
        let mut headers = HeaderMap::new();
        headers.insert(SEQ_NO, (seq_no + 1).to_string().as_ref());
        headers.insert(LEN, (len).to_string().as_ref());
        self.jetstream
            .publish_with_headers(subject, headers, bytes.into())
            .await
            .map_err(Error::PublishEvts)?
            .await
            .map_err(Error::PublishEvtsAck)?;
        debug!(%id, "Successfully published to NATS");

        Ok(())
    }

    async fn last_seq_no(&self, id: Uuid) -> Result<u64, Self::Error> {
        let stream_name = self.stream_name.as_str();
        let subject = format!("{stream_name}.{id}");
        let stream = self.get_stream().await?;
        stream
            .get_last_raw_message_by_subject(&subject)
            .await
            .map_or_else(
                |error| {
                    // TODO What the hell is this? Will async_nats improve error handling!
                    let source = *error
                        .downcast::<io::Error>()
                        .expect("Cannot downcast async_nats error")
                        .into_inner()
                        .expect("Missing inner error")
                        .downcast::<response::Error>()
                        .expect("Cannot convert to async_nats response error");
                    if source.code == 10037 {
                        Ok(0)
                    } else {
                        Err(Error::GetLastMessage(Box::new(source)))
                    }
                },
                |msg| {
                    let msg = Message::try_from(msg).map_err(Error::FromRawMessage);
                    msg.map(|ref msg| {
                        let (seq_no, len) = seq_no_and_len(msg);
                        seq_no + len - 1
                    })
                },
            )
    }

    async fn evts_by_id<E>(
        &mut self,
        id: Uuid,
        from_seq_no: u64,
        to_seq_no: u64,
    ) -> Result<impl Stream<Item = Result<E, Self::Error>>, Self::Error>
    where
        E: TryFromBytes + Send,
    {
        assert!(from_seq_no > 0, "from_seq_no must be positive");
        assert!(
            from_seq_no <= to_seq_no,
            "from_seq_no must be less than or equal to to_seq_no"
        );

        // Get message stream
        let msgs = self
            .get_stream()
            .await?
            .create_consumer(pull::Config {
                filter_subject: format!("{}.{id}", self.stream_name),
                ..Default::default()
            })
            .await
            .map_err(Error::CreateConsumer)?
            .messages()
            .await
            .map_err(Error::GetMessages)?;

        // Transform message stream into event stream
        let evts = msgs
            .map(move |msg| match msg {
                Ok(msg) => {
                    let (seq_no, len) = seq_no_and_len(&msg);
                    // Only convert if current message has relevant sequence number range.
                    if from_seq_no < seq_no + len {
                        let proto::Evts { evts } = proto::Evts::decode(msg.message.payload)?;
                        evts.into_iter()
                            .enumerate()
                            .map(|(n, evt)| {
                                E::try_from_bytes(evt)
                                    .map_err(|source| Error::EvtsFromBytes(Box::new(source)))
                                    .map(|evt| (seq_no + n as u64, evt))
                            })
                            .collect()
                    } else {
                        Ok(vec![])
                    }
                }
                Err(source) => Err(Error::GetMessage(source)),
            })
            .flat_map(|evts| match evts {
                Ok(evts) => stream::iter(evts.into_iter().map(Ok).collect::<Vec<_>>()),
                Err(error) => stream::iter(vec![Err(error)]),
            });

        // Respect sequence number range, in particular stop at `to_seq_no`.
        let evts = stream! {
            for await evt in evts {
                match evt {
                    Ok((n, _ev)) if n < from_seq_no => continue,
                    Ok((n, evt)) if from_seq_no <= n && n < to_seq_no => yield Ok(evt),
                    Ok((n, evt)) if n == to_seq_no => {
                        yield Ok(evt);
                        break;
                    }
                    Ok(_) => break, // to_seq_no < seq_no
                    Err(error) => yield Err(error),
                }
            }
        };

        Ok(evts)
    }
}

/// Configuration for the [NatsEvtLog].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    server_addr: String,
    stream_name: String,
}

impl Config {
    #[allow(missing_docs)]
    pub fn new<S, T>(server_addr: S, stream_name: T) -> Self
    where
        S: Into<String>,
        T: Into<String>,
    {
        Self {
            server_addr: server_addr.into(),
            stream_name: stream_name.into(),
        }
    }
}

impl Default for Config {
    /// Use "localhost:4222" for `server_addr` and "evts" for `subject_prefix`.
    fn default() -> Self {
        Self::new("localhost:4222", "evts")
    }
}

/// Errors from the [NatsEvtLog].
#[derive(Debug, Error)]
pub enum Error {
    /// The connection to the NATS server cannot be established.
    #[error("Cannot connect to NATS server")]
    Connect(#[from] std::io::Error),

    /// Events cannot be published.
    #[error("Cannot publish events")]
    PublishEvts(#[source] async_nats::Error),

    /// An ACK for publishing events cannot be received.
    #[error("Cannot get ACK for publishing events")]
    PublishEvtsAck(#[source] async_nats::Error),

    /// A NATS stream cannot be obtained.
    #[error("Cannot get NATS stream")]
    GetStream(#[source] async_nats::Error),

    /// A NATS consumer cannot be created.
    #[error("Cannot create NATS consumer")]
    CreateConsumer(#[source] async_nats::Error),

    /// The message stream from a NATS consumer cannot be obtained.
    #[error("Cannot get message stream from NATS consumer")]
    GetMessages(#[source] async_nats::Error),

    /// A message cannot be obtained from the NATS message stream.
    #[error("Cannot get message from NATS message stream")]
    GetMessage(#[source] async_nats::Error),

    /// The last message for a NATS stream cannot be obtained.
    #[error("Cannot get last message for NATS stream")]
    GetLastMessage(#[source] async_nats::Error),

    /// A raw NATS message cannot be converted into a NATS message.
    #[error("Cannot convert raw NATS message into NATS message")]
    FromRawMessage(#[source] async_nats::Error),

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
}

fn seq_no_and_len(msg: &Message) -> (u64, u64) {
    let seq_no = header(msg, SEQ_NO);
    let len = header(msg, LEN);
    (seq_no, len)
}

fn header<T>(msg: &Message, name: &str) -> T
where
    T: FromStr,
    <T as FromStr>::Err: Debug,
{
    // Unwrapping should always be successful, hence panicing is valid.
    msg.headers
        .as_ref()
        .expect("No headers")
        .get(name)
        .unwrap_or_else(|| panic!("Missing {name} header"))
        .iter()
        .next()
        .unwrap_or_else(|| panic!("Missing value for {name} header"))
        .parse::<T>()
        .unwrap_or_else(|_| panic!("{name} header cannot be parsed"))
}

mod proto {
    #![allow(clippy::derive_partial_eq_without_eq)]
    include!(concat!(env!("OUT_DIR"), "/evt_log.nats.rs"));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;
    use anyhow::anyhow;
    use futures::StreamExt;
    use testcontainers::{clients::Cli, core::WaitFor, images::generic::GenericImage, Container};

    fn setup_testcontainers(client: &Cli) -> Container<'_, GenericImage> {
        let nats_image = GenericImage::new("nats", "2.9.9")
            .with_wait_for(WaitFor::message_on_stderr("Server is ready"));
        let container = client.run((nats_image, vec!["-js".to_string()]));
        container
    }

    async fn setup_jetstream(server_addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        let client = connect(server_addr).await?;
        let jetstream = jetstream::new(client);
        let _ = jetstream
            .create_stream(jetstream::stream::Config {
                name: "evts".to_string(),
                subjects: vec!["evts.>".to_string()],
                ..Default::default()
            })
            .await
            .map_err(|error| anyhow!(error))?;
        Ok(())
    }

    async fn create_evt_log(server_addr: String) -> Result<NatsEvtLog, Box<dyn std::error::Error>> {
        let config = Config {
            server_addr,
            stream_name: "evts".to_string(),
        };
        let nats_event_log = NatsEvtLog::new(config).await?;
        Ok(nats_event_log)
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Cmd {
        Inc(u64),
        Dec(u64),
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    enum Evt {
        Increased { old_value: u64, inc: u64 },
        Decreased { old_value: u64, dec: u64 },
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
    enum Error {
        #[error("Overflow: value={value}, increment={inc}")]
        Overflow { value: u64, inc: u64 },
        #[error("Underflow: value={value}, decrement={dec}")]
        Underflow { value: u64, dec: u64 },
    }

    #[derive(Debug)]
    struct Counter(u64);

    impl EventSourced for Counter {
        type Cmd = Cmd;

        type Evt = Evt;

        type Error = Error;

        fn handle_cmd(&self, cmd: Self::Cmd) -> Result<Vec<Self::Evt>, Self::Error> {
            match cmd {
                Cmd::Inc(inc) => {
                    if inc > u64::MAX - self.0 {
                        Err(Error::Overflow { value: self.0, inc })
                    } else {
                        Ok(vec![Evt::Increased {
                            old_value: self.0,
                            inc,
                        }])
                    }
                }
                Cmd::Dec(dec) => {
                    if dec > self.0 {
                        Err(Error::Underflow { value: self.0, dec })
                    } else {
                        Ok(vec![Evt::Decreased {
                            old_value: self.0,
                            dec,
                        }])
                    }
                }
            }
        }

        fn handle_evt(&mut self, evt: &Self::Evt) {
            match evt {
                Evt::Increased { old_value: _, inc } => self.0 += inc,
                Evt::Decreased { old_value: _, dec } => self.0 -= dec,
            }
        }
    }

    /// Directly testing the [NatsEvtLog] with trivial events (`u64`).
    #[tokio::test]
    async fn test_evt_log() -> Result<(), Box<dyn std::error::Error>> {
        let client = Cli::default();
        let container = setup_testcontainers(&client);
        let server_addr = format!("localhost:{}", container.get_host_port_ipv4(4222));
        setup_jetstream(&server_addr).await?;
        let mut evt_log = create_evt_log(server_addr).await?;
        let id = Uuid::now_v7();

        // Verify `last_seq_no` for empty log.
        let last_seq_no = evt_log.last_seq_no(id).await?;
        assert_eq!(last_seq_no, 0);

        // Verify `persist`.
        evt_log.persist(id, &[1, 2, 3, 4], 1000).await?;
        evt_log.persist(id, &[5, 6, 7], 1004).await?;
        evt_log.persist(id, &[8, 9], 1007).await?;

        // Verify `last_seq_no` for non-empty log.
        let last_seq_no = evt_log.last_seq_no(id).await?;
        assert_eq!(last_seq_no, 1009);

        // Verify `evts_by_id`.
        let evts = evt_log.evts_by_id::<u32>(id, 1002, 1008).await?;
        let evts = evts.collect::<Vec<_>>().await;
        let evts = evts
            .into_iter()
            .filter_map(|evt| evt.ok())
            .collect::<Vec<_>>();
        assert_eq!(evts, (2..=8).collect::<Vec<_>>());

        Ok(())
    }

    /// Testing the [NatsEvtLog] via a "proper" [Entity].
    #[tokio::test]
    async fn test_entity() -> Result<(), Box<dyn std::error::Error>> {
        let client = Cli::default();
        let container = setup_testcontainers(&client);
        let server_addr = format!("localhost:{}", container.get_host_port_ipv4(4222));
        setup_jetstream(&server_addr).await?;
        let mut evt_log = create_evt_log(server_addr).await?;
        let id = Uuid::now_v7();

        // Populate the event log with some events.
        evt_log
            .persist(
                id,
                &[
                    Evt::Increased {
                        old_value: 0,
                        inc: 10,
                    },
                    Evt::Increased {
                        old_value: 10,
                        inc: 90,
                    },
                ],
                1,
            )
            .await?;

        // Create an entity.
        let counter = Entity::spawn(id, Counter(0), evt_log).await?;

        // Handle a valid command.
        let evts = counter.handle_cmd(Cmd::Inc(1)).await?;
        assert_eq!(
            evts,
            vec![Evt::Increased {
                old_value: 100,
                inc: 1
            }]
        );

        // Handle another valid command.
        let evts = counter.handle_cmd(Cmd::Dec(101)).await?;
        assert_eq!(
            evts,
            vec![Evt::Decreased {
                old_value: 101,
                dec: 101
            }]
        );

        // Handle an invalid command (underflow).
        let result = counter.handle_cmd(Cmd::Dec(1)).await;
        assert!(result.is_err());

        Ok(())
    }
}
