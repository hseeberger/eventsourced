//! An [EvtLog] implementation based on [NATS](https://nats.io/).

use crate::Error;
use async_nats::{
    connect,
    jetstream::{
        self,
        consumer::{pull, AckPolicy, DeliverPolicy},
        response,
        stream::Stream as JetstreamStream,
        Context as Jetstream,
    },
    HeaderMap, Message,
};
use async_stream::stream;
use bytes::{Bytes, BytesMut};
use eventsourced::{EvtLog, Metadata};
use futures::{stream, Stream, StreamExt};
use prost::Message as ProstMessage;
use serde::{Deserialize, Serialize};
use std::{
    error::Error as StdError,
    fmt::{self, Debug, Formatter},
    io,
    str::FromStr,
};
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

    pub async fn setup(&self) -> Result<(), Box<dyn StdError + Send + Sync>> {
        let _ = self
            .jetstream
            .create_stream(jetstream::stream::Config {
                name: "evts".to_string(),
                subjects: vec!["evts.>".to_string()],
                ..Default::default()
            })
            .await?;
        Ok(())
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

    async fn persist<'a, 'b, 'c, E, EvtToBytes, EvtToBytesError>(
        &'a self,
        id: Uuid,
        evts: &'b [E],
        last_seq_no: u64,
        evt_to_bytes: &'c EvtToBytes,
    ) -> Result<Metadata, Self::Error>
    where
        'b: 'a,
        'c: 'a,
        E: Send + Sync + 'a,
        EvtToBytes: Fn(&E) -> Result<Bytes, EvtToBytesError> + Send + Sync,
        EvtToBytesError: StdError + Send + Sync + 'static,
    {
        assert!(!evts.is_empty(), "evts must not be empty");

        // Convert events into bytes.
        let len = evts.len();
        let evts = evts
            .iter()
            .map(evt_to_bytes)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| Error::EvtsIntoBytes(Box::new(source)))?;
        let evts = proto::Evts { evts };
        let mut bytes = BytesMut::new();
        evts.encode(&mut bytes)?;

        // Determine NATS subject.
        let stream_name = &self.stream_name;
        let subject = format!("{stream_name}.{id}");

        // Publish events to NATS subject and await ACK.
        let mut headers = HeaderMap::new();
        headers.insert(SEQ_NO, (last_seq_no + 1).to_string().as_ref());
        headers.insert(LEN, (len).to_string().as_ref());
        let ack = self
            .jetstream
            .publish_with_headers(subject, headers, bytes.into())
            .await
            .map_err(Error::PublishEvts)?
            .await
            .map_err(Error::PublishEvtsAck)?;
        let metadata = Box::new(ack.sequence);

        Ok(Some(metadata))
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
                        debug!(%id, "No last msg found");
                        Ok(0)
                    } else {
                        Err(Error::GetLastMessage(Box::new(source)))
                    }
                },
                |msg| {
                    let msg = Message::try_from(msg).map_err(Error::FromRawMessage);
                    msg.map(|ref msg| {
                        let (seq_no, len) = seq_no_and_len(msg);
                        debug!(%id, seq_no, len, "Last msg found");
                        seq_no + len - 1
                    })
                },
            )
    }

    async fn evts_by_id<'a, E, EvtFromBytes, EvtFromBytesError>(
        &'a self,
        id: Uuid,
        from_seq_no: u64,
        to_seq_no: u64,
        metadata: Metadata,
        evt_from_bytes: EvtFromBytes,
    ) -> Result<impl Stream<Item = Result<(u64, E), Self::Error>> + Send, Self::Error>
    where
        E: Send + 'a,
        EvtFromBytes: Fn(Bytes) -> Result<E, EvtFromBytesError> + Copy + Send + Sync + 'static,
        EvtFromBytesError: StdError + Send + Sync + 'static,
    {
        assert!(from_seq_no > 0, "from_seq_no must be positive");
        assert!(
            from_seq_no <= to_seq_no,
            "from_seq_no must be less than or equal to to_seq_no"
        );

        debug!(%id, from_seq_no, to_seq_no, "Building event stream");

        // Get message stream
        let deliver_policy =
            match metadata.and_then(|metadata| metadata.downcast_ref::<u64>().copied()) {
                Some(start_sequence) => DeliverPolicy::ByStartSequence { start_sequence },
                None => DeliverPolicy::All,
            };
        let msgs = self
            .get_stream()
            .await?
            .create_consumer(pull::Config {
                filter_subject: format!("{}.{id}", self.stream_name),
                ack_policy: AckPolicy::None, // Important!
                deliver_policy,
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
                                evt_from_bytes(evt)
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
                    Ok((n, _evt)) if n < from_seq_no => continue,
                    Ok((n, evt)) if from_seq_no <= n && n < to_seq_no => yield Ok((n, evt)),
                    Ok((n, evt)) if n == to_seq_no => {
                        yield Ok((n, evt));
                        break;
                    }
                    Ok(_) => break, // to_seq_no < seq_no
                    Err(error) => {
                        yield Err(error);
                        break;
                    },
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
}

impl Default for Config {
    /// Use "localhost:4222" for `server_addr` and "evts" for `stream_name`.
    fn default() -> Self {
        let server_addr = "localhost:4222".into();
        let stream_name = "evts".into();
        Self {
            server_addr,
            stream_name,
        }
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
    include!(concat!(env!("OUT_DIR"), "/evt_log.rs"));
}

#[cfg(all(test))]
mod tests {
    use super::*;
    use eventsourced::convert;
    use futures::TryStreamExt;
    use std::future;
    use testcontainers::{clients::Cli, core::WaitFor, images::generic::GenericImage};

    #[tokio::test]
    async fn test_evt_log() -> Result<(), Box<dyn StdError + Send + Sync>> {
        let client = Cli::default();
        let nats_image = GenericImage::new("nats", "2.9.9")
            .with_wait_for(WaitFor::message_on_stderr("Server is ready"));
        let container = client.run((nats_image, vec!["-js".to_string()]));
        let server_addr = format!("localhost:{}", container.get_host_port_ipv4(4222));

        let config = Config::default().with_server_addr(server_addr);
        let evt_log = NatsEvtLog::new(config).await?;
        evt_log.setup().await?;

        let id = Uuid::now_v7();

        let last_seq_no = evt_log.last_seq_no(id).await?;
        assert_eq!(last_seq_no, 0);

        evt_log
            .persist(id, [1, 2, 3].as_ref(), 0, &convert::prost::to_bytes)
            .await?;
        let last_seq_no = evt_log.last_seq_no(id).await?;
        assert_eq!(last_seq_no, 3);

        let evts = evt_log
            .evts_by_id::<i32, _, _>(id, 2, 3, None, convert::prost::from_bytes)
            .await?;
        let sum = evts
            .try_fold(0i32, |acc, (_, n)| future::ready(Ok(acc + n)))
            .await?;
        assert_eq!(sum, 5);

        let evts = evt_log
            .evts_by_id::<i32, _, _>(id, 1, 5, None, convert::prost::from_bytes)
            .await?;

        evt_log
            .persist(id, [4, 5].as_ref(), 3, &convert::prost::to_bytes)
            .await?;
        let last_seq_no = evt_log.last_seq_no(id).await?;
        assert_eq!(last_seq_no, 5);

        let sum = evts
            .try_fold(0i32, |acc, (_, n)| future::ready(Ok(acc + n)))
            .await?;
        assert_eq!(sum, 15);

        Ok(())
    }
}
