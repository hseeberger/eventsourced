//! An [EvtLog] implementation based on [NATS](https://nats.io/).

use crate::Error;
use async_nats::{
    connect,
    jetstream::{
        self,
        consumer::{pull, AckPolicy, DeliverPolicy},
        response::{self, Response},
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
use serde_json::{json, Value};
use std::{
    collections::HashSet,
    error::Error as StdError,
    fmt::{self, Debug, Formatter},
    io,
    num::NonZeroUsize,
    str::FromStr,
};
use tokio::sync::broadcast;
use tracing::debug;
use uuid::Uuid;

const SEQ_NO: &str = "seq_no";
const LEN: &str = "len";

/// An [EvtLog] implementation based on [NATS](https://nats.io/).
#[derive(Clone)]
pub struct NatsEvtLog {
    stream_name: String,
    jetstream: Jetstream,
    id_sender: broadcast::Sender<Uuid>,
    ids: HashSet<Uuid>,
}

impl NatsEvtLog {
    #[allow(missing_docs)]
    pub async fn new(config: Config) -> Result<Self, Error> {
        debug!(?config, "Creating NatsEvtLog");

        let server_addr = config.server_addr;
        let client = connect(&server_addr).await?;
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
                .map_err(Error::CreateStream)?;
        }

        // Create ID broadcast sender and load IDs.
        let (id_sender, _) = broadcast::channel(config.id_broadcast_capacity.get());
        let subject = format!("STREAM.INFO.{}", "evts");
        let payload = &json!({ "subjects_filter": "evts.*" });
        let response = jetstream
            .request::<_, Value>(subject, payload)
            .await
            .map_err(Error::JetstreamRequest)?;
        let response = match response {
            Response::Ok(value) => Ok(value),
            Response::Err { error } => Err(Error::JetstreamResponse(error)),
        }?;
        // TODO: This is a little sloppy; in theory `state` might be missing.
        let ids = match &response["state"]["subjects"] {
            Value::Object(map) => {
                let ids = map
                    .keys()
                    .map(|key| {
                        key.strip_prefix("evts.")
                            .ok_or(Error::EvtsPrefixMissing)
                            .and_then(|key| key.try_into().map_err(Error::InvalidSubjectName))
                    })
                    .collect::<Result<HashSet<Uuid>, _>>();
                ids
            }
            // TODO: This is a little sloppy; in theory there could be other variants than `Null`.
            _ => Ok(HashSet::new()),
        }?;

        Ok(Self {
            stream_name: config.stream_name,
            jetstream,
            id_sender,
            ids,
        })
    }

    async fn get_stream(&self) -> Result<JetstreamStream, Error> {
        self.jetstream
            .get_stream(&self.stream_name)
            .await
            .map_err(Error::GetStream)
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

    async fn persist<'a, E, EvtToBytes, EvtToBytesError>(
        &'a mut self,
        id: Uuid,
        evts: &'a [E],
        last_seq_no: u64,
        evt_to_bytes: &'a EvtToBytes,
    ) -> Result<Metadata, Self::Error>
    where
        E: Debug + Send + Sync + 'a,
        EvtToBytes: Fn(&E) -> Result<Bytes, EvtToBytesError> + Send + Sync,
        EvtToBytesError: StdError + Send + Sync + 'static,
    {
        assert!(!evts.is_empty(), "evts must not be empty");
        assert!(
            last_seq_no <= Self::MAX_SEQ_NO - evts.len() as u64,
            "last_seq_no must be less or equal {} - evts.len()",
            Self::MAX_SEQ_NO
        );

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

        // Publish events to NATS subject and await ACK.
        let subject = format!("{}.{id}", self.stream_name);
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

        // Add ID to IDs and send it to ID broadcast channel.
        self.ids.insert(id);
        let _ = self.id_sender.send(id);

        Ok(Some(metadata))
    }

    async fn last_seq_no(&self, id: Uuid) -> Result<u64, Self::Error> {
        let subject = format!("{}.{id}", self.stream_name);
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
        E: Debug + Send + 'a,
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
        let subject = format!("{}.{id}", self.stream_name);
        let msgs = self
            .get_stream()
            .await?
            .create_consumer(pull::Config {
                filter_subject: subject,
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

    async fn ids(&self) -> Result<impl Stream<Item = Uuid> + '_, Self::Error> {
        let mut id_receiver = self.id_sender.subscribe();

        let ids = stream! {
            // Yield current IDs.
            for id in &self.ids { yield *id; }

            // Yield future IDs after deduplication.
            while let Ok(id) = id_receiver.recv().await {
                if !self.ids.contains(&id) {
                    yield id;
                }
            }
        };

        Ok(ids)
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

        let config = Config::default()
            .with_server_addr(server_addr)
            .with_setup(true);
        let mut evt_log = NatsEvtLog::new(config).await?;

        let id = Uuid::now_v7();

        // Start testing.

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

        let ids = evt_log.ids().await?;
        let ids = ids.take(1).collect::<HashSet<_>>().await;
        assert_eq!(ids, HashSet::from_iter(vec![id]));
        let ids = evt_log.ids().await?;

        let evts = evt_log
            .evts_by_id::<i32, _, _>(
                id,
                1,
                NatsEvtLog::MAX_SEQ_NO,
                None,
                convert::prost::from_bytes,
            )
            .await?;

        evt_log
            .clone()
            .persist(id, [4, 5].as_ref(), 3, &convert::prost::to_bytes)
            .await?;
        let last_seq_no = evt_log.last_seq_no(id).await?;
        assert_eq!(last_seq_no, 5);

        let sum = evts
            .take(5)
            .try_fold(0i32, |acc, (_, n)| future::ready(Ok(acc + n)))
            .await?;
        assert_eq!(sum, 15);

        let ids = ids.take(1).collect::<HashSet<_>>().await;
        assert_eq!(ids, HashSet::from_iter(vec![id]));
        let ids = evt_log.ids().await?;

        let id_2 = Uuid::now_v7();

        evt_log
            .clone()
            .persist(id_2, [1].as_ref(), 0, &convert::prost::to_bytes)
            .await?;

        let ids = ids.take(2).collect::<HashSet<_>>().await;
        assert_eq!(ids, HashSet::from_iter(vec![id, id_2]));

        Ok(())
    }
}
