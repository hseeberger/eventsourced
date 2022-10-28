use super::EvtLog;
use async_nats::{
    connect,
    jetstream::{
        self, consumer::pull, response, stream::Stream as JetstreamStream, Context as Jetstream,
    },
    HeaderMap, Message,
};
use async_stream::stream;
use futures::{stream, Stream, StreamExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
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

#[derive(Debug, Error)]
pub enum Error {
    #[error("Cannot connect to NATS server at '{server_addr}'")]
    Connect {
        server_addr: String,
        source: std::io::Error,
    },

    #[error("Cannot serialize events for entity ID '{id}'")]
    Serialize { id: Uuid, source: serde_json::Error },

    #[error("Cannot publish event for entity ID '{id}'")]
    Publish { id: Uuid, source: async_nats::Error },

    #[error("Cannot get ACK for publishing event for entity ID '{id}'")]
    PublishAck { id: Uuid, source: async_nats::Error },

    #[error("Cannot get NATS stream with name '{stream_name}'")]
    Stream {
        stream_name: String,
        source: async_nats::Error,
    },

    #[error("Cannot create consumer for NATS stream with name '{stream_name}'")]
    Consumer {
        stream_name: String,
        source: async_nats::Error,
    },

    #[error("Cannot get messages from consumer for NATS stream with name '{stream_name}'")]
    Messages {
        stream_name: String,
        source: async_nats::Error,
    },

    #[error("Cannot get message from consumer for NATS stream with name '{stream_name}'")]
    Message {
        stream_name: String,
        source: async_nats::Error,
    },

    #[error("Cannot deserialize message from consumer for NATS stream with name '{stream_name}'")]
    DeserializeMessage {
        stream_name: String,
        source: serde_json::Error,
    },

    #[error(
        "Cannot get last message for NATS stream with name '{stream_name}' and subject '{subject}'"
    )]
    GetLastMessage {
        stream_name: String,
        subject: String,
        source: async_nats::Error,
    },

    #[error("Cannot convert raw message into message")]
    FromRawMessage { source: async_nats::Error },

    #[error("Dummy")]
    Dummy,
}

/// An [EventLog] implementation based on [NATS](https://nats.io/).
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
        let client = connect(&server_addr)
            .await
            .map_err(|source| Error::Connect {
                server_addr,
                source,
            })?;
        let jetstream = jetstream::new(client);

        Ok(Self {
            jetstream,
            stream_name: config.stream_name,
        })
    }

    async fn get_stream(&self, stream_name: &str) -> Result<JetstreamStream, Error> {
        self.jetstream
            .get_stream(stream_name)
            .await
            .map_err(|source| Error::Stream {
                stream_name: stream_name.to_string(),
                source,
            })
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
        E: Debug + Serialize + Send + Sync + 'a,
    {
        if evts.is_empty() {
            debug!(%id, "Not publishing to NATS, because no events given");
            return Ok(());
        }

        // Serialize events.
        let evts_json =
            serde_json::to_value(evts).map_err(|source| Error::Serialize { id, source })?;

        // Determine NATS subject.
        let stream_name = &self.stream_name;
        let subject = format!("{stream_name}.{id}");
        debug!(%id, ?subject, ?evts_json, "Publishing to NATS");

        // Publish events to NATS subject and await ACK.
        let mut headers = HeaderMap::new();
        headers.insert(SEQ_NO, (seq_no + 1).to_string().as_ref());
        headers.insert(LEN, (evts.len()).to_string().as_ref());
        self.jetstream
            .publish_with_headers(subject, headers, evts_json.to_string().into())
            .await
            .map_err(|source| Error::Publish { id, source })?
            .await
            .map_err(|source| Error::PublishAck { id, source })?;
        debug!(%id, ?evts_json, "Successfully published to NATS");

        Ok(())
    }

    async fn last_seq_no(&self, id: Uuid) -> Result<u64, Self::Error> {
        let stream_name = self.stream_name.as_str();
        let subject = format!("{stream_name}.{id}");
        let stream = self.get_stream(stream_name).await?;
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
                        Err(Error::GetLastMessage {
                            stream_name: stream_name.to_owned(),
                            subject,
                            source: Box::new(source),
                        })
                    }
                },
                |msg| {
                    let msg =
                        Message::try_from(msg).map_err(|source| Error::FromRawMessage { source });
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
        E: Debug + DeserializeOwned + Send,
    {
        let stream_name = self.stream_name.clone();
        let subject = format!("{stream_name}.{id}");

        let stream = self.get_stream(&stream_name).await?;

        let consumer = stream
            .create_consumer(pull::Config {
                filter_subject: subject,
                ..Default::default()
            })
            .await
            .map_err(|source| Error::Consumer {
                stream_name: stream_name.clone(),
                source,
            })?;

        let msgs = consumer
            .messages()
            .await
            .map_err(|source| Error::Messages {
                stream_name: stream_name.clone(),
                source,
            })?;

        let evts = msgs
            .map(move |msg| match msg {
                Ok(msg) => {
                    let (seq_no, len) = seq_no_and_len(&msg);
                    // Only deserialize if current message has relevant sequence numbers
                    if from_seq_no < seq_no + len {
                        serde_json::from_slice::<Vec<E>>(&msg.payload)
                            .map_err(|source| Error::DeserializeMessage {
                                stream_name: stream_name.clone(),
                                source,
                            })
                            .map(|evts| {
                                evts.into_iter()
                                    .enumerate()
                                    .map(|(n, evt)| (seq_no + n as u64, evt))
                                    .collect::<Vec<_>>()
                            })
                    } else {
                        Ok(vec![])
                    }
                }
                Err(source) => Err(Error::Message {
                    stream_name: stream_name.clone(),
                    source,
                }),
            })
            .flat_map(|evts| match evts {
                Ok(evts) => stream::iter(evts.into_iter().map(Ok).collect::<Vec<_>>()),
                Err(error) => stream::iter(vec![Err(error)]),
            });

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;
    use futures::StreamExt;
    use testcontainers::{clients::Cli, core::WaitFor, images::generic::GenericImage, Container};

    fn setup_testcontainers(client: &Cli) -> Container<'_, GenericImage> {
        let nats_image = GenericImage::new("nats", "2.9.8")
            .with_wait_for(WaitFor::message_on_stderr("Server is ready"));
        let container = client.run((nats_image, vec!["-js".to_string()]));
        container
    }

    async fn setup_jetstream(server_addr: &str) {
        let client = connect(server_addr).await;
        let client = client.unwrap();
        let jetstream = jetstream::new(client);
        jetstream
            .create_stream(jetstream::stream::Config {
                name: "evts".to_string(),
                subjects: vec!["evts.>".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
    }

    async fn create_evt_log(server_addr: String) -> NatsEvtLog {
        let config = Config {
            server_addr,
            stream_name: "evts".to_string(),
        };
        let event_log = NatsEvtLog::new(config).await;
        assert!(event_log.is_ok());
        event_log.unwrap()
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
    async fn test_evt_log() {
        let client = Cli::default();
        let container = setup_testcontainers(&client);
        let server_addr = format!("localhost:{}", container.get_host_port_ipv4(4222));
        setup_jetstream(&server_addr).await;
        let mut evt_log = create_evt_log(server_addr).await;
        let id = Uuid::now_v7();

        // Verify `last_seq_no` for empty log.
        let last_seq_no = evt_log.last_seq_no(id).await;
        assert!(last_seq_no.is_ok());
        let last_seq_no = last_seq_no.unwrap();
        assert_eq!(last_seq_no, 0);

        // Verify `persist`.
        let result = evt_log.persist(id, &[1, 2, 3, 4], 1000).await;
        assert!(result.is_ok());
        let result = evt_log.persist(id, &[5, 6, 7], 1004).await;
        assert!(result.is_ok());
        let result = evt_log.persist(id, &[8, 9], 1007).await;
        assert!(result.is_ok());

        // Verify `last_seq_no` for non-empty log.
        let last_seq_no = evt_log.last_seq_no(id).await;
        assert!(last_seq_no.is_ok());
        let last_seq_no = last_seq_no.unwrap();
        assert_eq!(last_seq_no, 1009);

        // Verify `evts_by_id`.
        let evts = evt_log.evts_by_id::<u32>(id, 1002, 1008).await;
        assert!(evts.is_ok());
        let evts = evts.unwrap();
        let evts = evts.collect::<Vec<_>>().await;
        let evts = evts
            .into_iter()
            .filter_map(|evt| evt.ok())
            .collect::<Vec<_>>();
        assert_eq!(evts, (2..=8).collect::<Vec<_>>());
    }

    /// Testing the [NatsEvtLog] via a "proper" [Entity].
    #[tokio::test]
    async fn test_entity() {
        let client = Cli::default();
        let container = setup_testcontainers(&client);
        let server_addr = format!("localhost:{}", container.get_host_port_ipv4(4222));
        setup_jetstream(&server_addr).await;
        let mut evt_log = create_evt_log(server_addr).await;
        let id = Uuid::now_v7();

        // Populate the event log with some events.
        let result = evt_log
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
            .await;
        assert!(result.is_ok());

        // Create the entity.
        let entity = Entity::spawn(id, Counter(0), evt_log).await;
        assert!(entity.is_ok());
        let counter = entity.unwrap();

        // Handle a valid command.
        let evts = counter.handle_cmd(Cmd::Inc(1)).await;
        assert!(evts.is_ok());
        let evts = evts.unwrap();
        assert_eq!(
            evts,
            vec![Evt::Increased {
                old_value: 100,
                inc: 1
            }]
        );

        // Handle another valid command.
        let evts = counter.handle_cmd(Cmd::Dec(101)).await;
        assert!(evts.is_ok());
        let evts = evts.unwrap();
        assert_eq!(
            evts,
            vec![Evt::Decreased {
                old_value: 101,
                dec: 101
            }]
        );

        // Handle an invalid command (underflow).
        let evts = counter.handle_cmd(Cmd::Dec(1)).await;
        assert!(evts.is_err());
    }
}
