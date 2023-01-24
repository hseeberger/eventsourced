//! An [EvtLog] implementation based on [NATS](https://nats.io/).

use crate::Error;
use async_nats::{
    connect,
    jetstream::{
        self,
        consumer::{pull, AckPolicy, DeliverPolicy},
        response::{self},
        stream::Stream as JetstreamStream,
        Context as Jetstream,
    },
    HeaderMap, Message,
};
use async_stream::stream;
use bytes::Bytes;
use eventsourced::{EvtLog, Metadata};
use futures::{stream, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    error::Error as StdError,
    fmt::{self, Debug, Formatter},
    future::ready,
    io,
    num::{NonZeroU64, NonZeroUsize},
    str::FromStr,
};

use tracing::debug;
use uuid::Uuid;

const SEQ_NO: &str = "eventsourced-seq-no";

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

        Ok(Self {
            stream_name: config.stream_name,
            jetstream,
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
        evt: &'a E,
        tag: Option<String>,
        last_seq_no: u64,
        evt_to_bytes: &'a EvtToBytes,
    ) -> Result<Metadata, Self::Error>
    where
        E: Debug + Send + Sync + 'a,
        EvtToBytes: Fn(&E) -> Result<Bytes, EvtToBytesError> + Send + Sync,
        EvtToBytesError: StdError + Send + Sync + 'static,
    {
        // Convert event into bytes.
        let bytes = evt_to_bytes(evt).map_err(|source| Error::EvtsIntoBytes(Box::new(source)))?;

        // Publish event to NATS subject and await ACK.
        let subject = format!("{}.{id}", self.stream_name);
        let mut headers = HeaderMap::new();
        headers.insert(SEQ_NO, (last_seq_no + 1).to_string().as_ref());
        let ack = self
            .jetstream
            .publish_with_headers(subject, headers, bytes)
            .await
            .map_err(Error::PublishEvts)?
            .await
            .map_err(Error::PublishEvtsAck)?;
        let metadata = Box::new(ack.sequence);

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
                    Message::try_from(msg)
                        .map_err(Error::FromRawMessage)
                        .map(|msg| {
                            let seq_no = seq_no(&msg);
                            debug!(%id, seq_no, "Last msg found");
                            seq_no
                        })
                },
            )
    }

    async fn evts_by_id<'a, E, EvtFromBytes, EvtFromBytesError>(
        &'a self,
        id: Uuid,
        from_seq_no: NonZeroU64,
        to_seq_no: u64,
        metadata: Metadata,
        evt_from_bytes: EvtFromBytes,
    ) -> Result<impl Stream<Item = Result<(u64, E), Self::Error>> + Send, Self::Error>
    where
        E: Debug + Send + 'a,
        EvtFromBytes: Fn(Bytes) -> Result<E, EvtFromBytesError> + Copy + Send + Sync + 'static,
        EvtFromBytesError: StdError + Send + Sync + 'static,
    {
        assert!(
            from_seq_no.get() <= to_seq_no,
            "from_seq_no must be less than or equal to to_seq_no"
        );

        debug!(%id, from_seq_no, to_seq_no, "Building events by ID stream");

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
        let evts = msgs.filter_map(move |msg| {
            ready(
                msg.map_err(|source| Error::GetMessage(source))
                    .and_then(|msg| {
                        let seq_no = seq_no(&msg);
                        if from_seq_no.get() <= seq_no {
                            evt_from_bytes(msg.message.payload)
                                .map_err(|source| Error::EvtsFromBytes(Box::new(source)))
                                .map(|evt| Some((seq_no, evt)))
                        } else {
                            Ok(None)
                        }
                    })
                    .transpose(),
            )
        });

        // Respect sequence number range, in particular stop at `to_seq_no`.
        let evts = stream! {
            for await evt in evts {
                match evt {
                    Ok((n, _evt)) if n < from_seq_no.get() => continue,
                    Ok((n, evt)) if from_seq_no.get() <= n && n < to_seq_no => yield Ok((n, evt)),
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

    async fn evts_by_tag<'a, E, EvtFromBytes, EvtFromBytesError>(
        &'a self,
        tag: String,
        from_offset: u64,
        evt_from_bytes: EvtFromBytes,
    ) -> Result<impl Stream<Item = Result<(u64, E), Self::Error>> + Send + '_, Self::Error>
    where
        E: Send + 'a,
        EvtFromBytes: Fn(Bytes) -> Result<E, EvtFromBytesError> + Copy + Send + Sync + 'static,
        EvtFromBytesError: StdError + Send + Sync + 'static,
    {
        debug!(tag, from_offset, "Building events by tag stream");

        Ok(stream::empty())
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

fn seq_no(msg: &Message) -> u64 {
    header(msg, SEQ_NO)
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
            .persist(id, &1, None, 0, &convert::prost::to_bytes)
            .await?;
        evt_log
            .persist(id, &2, None, 1, &convert::prost::to_bytes)
            .await?;
        evt_log
            .persist(id, &3, None, 2, &convert::prost::to_bytes)
            .await?;
        let last_seq_no = evt_log.last_seq_no(id).await?;
        assert_eq!(last_seq_no, 3);

        let evts = evt_log
            .evts_by_id::<i32, _, _>(
                id,
                unsafe { NonZeroU64::new_unchecked(2) },
                3,
                None,
                convert::prost::from_bytes,
            )
            .await?;
        let sum = evts
            .try_fold(0i32, |acc, (_, n)| future::ready(Ok(acc + n)))
            .await?;
        assert_eq!(sum, 5);

        let evts = evt_log
            .evts_by_id::<i32, _, _>(
                id,
                unsafe { NonZeroU64::new_unchecked(1) },
                NatsEvtLog::MAX_SEQ_NO,
                None,
                convert::prost::from_bytes,
            )
            .await?;

        evt_log
            .clone()
            .persist(id, &4, None, 3, &convert::prost::to_bytes)
            .await?;
        evt_log
            .clone()
            .persist(id, &5, None, 4, &convert::prost::to_bytes)
            .await?;
        let last_seq_no = evt_log.last_seq_no(id).await?;
        assert_eq!(last_seq_no, 5);

        let sum = evts
            .take(5)
            .try_fold(0i32, |acc, (_, n)| future::ready(Ok(acc + n)))
            .await?;
        assert_eq!(sum, 15);

        Ok(())
    }
}
