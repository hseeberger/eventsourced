//! An [EvtLog] implementation based on [PostgreSQL](https://www.postgresql.org/).

use crate::{Cnn, CnnPool, Error};
use async_stream::stream;
use bb8_postgres::{bb8::Pool, PostgresConnectionManager};
use bytes::Bytes;
use eventsourced::{EvtLog, SeqNo};
use futures::{Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::{
    error::Error as StdError,
    fmt::{self, Debug, Formatter},
    num::{NonZeroU64, NonZeroUsize},
    time::Duration,
};
use tokio::time::sleep;
use tokio_postgres::{types::ToSql, NoTls};
use tracing::debug;
use uuid::Uuid;

/// An [EvtLog] implementation based on [PostgreSQL](https://www.postgresql.org/).
#[derive(Clone)]
pub struct PostgresEvtLog {
    poll_interval: Duration,
    cnn_pool: CnnPool<NoTls>,
}

impl PostgresEvtLog {
    #[allow(missing_docs)]
    pub async fn new(config: Config) -> Result<Self, Error> {
        debug!(?config, "Creating PostgresEvtLog");

        // Create connection pool.
        let tls = NoTls;
        let cnn_manager = PostgresConnectionManager::new_from_stringlike(config.cnn_config(), tls)
            .map_err(Error::ConnectionManager)?;
        let cnn_pool = Pool::builder()
            .build(cnn_manager)
            .await
            .map_err(Error::ConnectionPool)?;

        // Setup tables.
        if config.setup {
            cnn_pool
                .get()
                .await
                .map_err(Error::GetConnection)?
                .batch_execute(
                    &include_str!("create_evt_log.sql").replace("evts", &config.evts_table),
                )
                .await
                .map_err(Error::ExecuteQuery)?;
        }

        Ok(Self {
            poll_interval: config.poll_interval,
            cnn_pool,
        })
    }

    async fn cnn(&self) -> Result<Cnn<NoTls>, Error> {
        self.cnn_pool.get().await.map_err(Error::GetConnection)
    }

    async fn next_evts_by_id<E, EvtFromBytes, EvtFromBytesError>(
        &self,
        id: Uuid,
        from_seq_no: SeqNo,
        to_seq_no: SeqNo,
        evt_from_bytes: EvtFromBytes,
    ) -> Result<impl Stream<Item = Result<(SeqNo, E), Error>> + Send, Error>
    where
        E: Send,
        EvtFromBytes: Fn(Bytes) -> Result<E, EvtFromBytesError> + Copy + Send + Sync + 'static,
        EvtFromBytesError: StdError + Send + Sync + 'static,
    {
        debug!(%id, %from_seq_no, %to_seq_no, "Querying events");

        let params: [&(dyn ToSql + Sync); 3] = [
            &id,
            &(from_seq_no.as_u64() as i64),
            &(to_seq_no.as_u64() as i64),
        ];
        let evts = self
            .cnn()
            .await?
            .query_raw(
                "SELECT seq_no, evt FROM evts WHERE id = $1 AND seq_no >= $2 AND seq_no <= $3",
                params,
            )
            .await
            .map_err(Error::ExecuteQuery)?
            .map_err(Error::NextRow)
            .map(move |row| {
                row.and_then(|row| {
                    let seq_no = (row.get::<_, i64>(0) as u64)
                        .try_into()
                        .map_err(Error::InvalidSeqNo)?;
                    let bytes = row.get::<_, &[u8]>(1);
                    let bytes = Bytes::copy_from_slice(bytes);
                    evt_from_bytes(bytes)
                        .map_err(|source| Error::FromBytes(Box::new(source)))
                        .map(|evt| (seq_no, evt))
                })
            });

        Ok(evts)
    }

    async fn next_evts_by_tag<E, EvtFromBytes, EvtFromBytesError>(
        &self,
        tag: &str,
        from_seq_no: SeqNo,
        evt_from_bytes: EvtFromBytes,
    ) -> Result<impl Stream<Item = Result<(SeqNo, E), Error>> + Send, Error>
    where
        E: Send,
        EvtFromBytes: Fn(Bytes) -> Result<E, EvtFromBytesError> + Copy + Send + Sync + 'static,
        EvtFromBytesError: StdError + Send + Sync + 'static,
    {
        debug!(tag, %from_seq_no, "Querying events");

        let params: [&(dyn ToSql + Sync); 2] = [&tag, &(from_seq_no.as_u64() as i64)];
        let evts = self
            .cnn()
            .await?
            .query_raw(
                "SELECT seq_no, evt FROM evts WHERE tag = $1 AND seq_no >= $2 ORDER BY seq_no",
                params,
            )
            .await
            .map_err(Error::ExecuteQuery)?
            .map_err(Error::NextRow)
            .map(move |row| {
                row.and_then(|row| {
                    let seq_no = (row.get::<_, i64>(0) as u64)
                        .try_into()
                        .map_err(Error::InvalidSeqNo)?;
                    let bytes = row.get::<_, &[u8]>(1);
                    let bytes = Bytes::copy_from_slice(bytes);
                    evt_from_bytes(bytes)
                        .map_err(|source| Error::FromBytes(Box::new(source)))
                        .map(|evt| (seq_no, evt))
                })
            });

        Ok(evts)
    }
}

impl Debug for PostgresEvtLog {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresEvtLog").finish()
    }
}

impl EvtLog for PostgresEvtLog {
    type Error = Error;

    /// The maximum value for sequence numbers. As PostgreSQL does not support unsigned integers,
    /// this is `i64::MAX` or `9_223_372_036_854_775_807`.
    const MAX_SEQ_NO: SeqNo = SeqNo::new(unsafe { NonZeroU64::new_unchecked(i64::MAX as u64) });

    async fn persist<'a, E, EvtToBytes, EvtToBytesError>(
        &'a mut self,
        id: Uuid,
        evt: &'a E,
        tag: Option<String>,
        evt_to_bytes: &'a EvtToBytes,
    ) -> Result<SeqNo, Self::Error>
    where
        E: Debug + Send + Sync + 'a,
        EvtToBytes: Fn(&E) -> Result<Bytes, EvtToBytesError> + Send + Sync,
        EvtToBytesError: StdError + Send + Sync + 'static,
    {
        debug!(%id, "Persisting event");

        // Persist event.
        let bytes = evt_to_bytes(evt).map_err(|source| Error::ToBytes(Box::new(source)))?;
        self.cnn()
            .await?
            .query_one(
                "INSERT INTO evts (id, evt, tag) VALUES ($1, $2, $3) RETURNING seq_no",
                &[&id, &bytes.as_ref(), &tag],
            )
            .await
            .map_err(Error::ExecuteQuery)
            .and_then(|row| {
                (row.get::<_, i64>(0) as u64)
                    .try_into()
                    .map_err(Error::InvalidSeqNo)
            })
    }

    async fn last_seq_no(&self, id: Uuid) -> Result<Option<SeqNo>, Self::Error> {
        self.cnn()
            .await?
            .query_one("SELECT MAX(seq_no) FROM evts WHERE id = $1", &[&id])
            .await
            .map_err(Error::ExecuteQuery)
            .and_then(|row| {
                // If there is no seq_no there is one row with a NULL column, hence use `try_get`.
                row.try_get::<_, i64>(0)
                    .ok()
                    .map(|seq_no| (seq_no as u64).try_into().map_err(Error::InvalidSeqNo))
                    .transpose()
            })
    }

    async fn evts_by_id<'a, E, EvtFromBytes, EvtFromBytesError>(
        &'a self,
        id: Uuid,
        from_seq_no: SeqNo,
        to_seq_no: SeqNo,
        evt_from_bytes: EvtFromBytes,
    ) -> Result<impl Stream<Item = Result<(SeqNo, E), Self::Error>> + Send + '_, Self::Error>
    where
        E: Debug + Send + 'a,
        EvtFromBytes: Fn(Bytes) -> Result<E, EvtFromBytesError> + Copy + Send + Sync + 'static,
        EvtFromBytesError: StdError + Send + Sync + 'static,
    {
        assert!(
            to_seq_no <= Self::MAX_SEQ_NO,
            "to_seq_no must be less or equal {}",
            Self::MAX_SEQ_NO
        );
        assert!(
            from_seq_no <= to_seq_no,
            "from_seq_no must be less than or equal to to_seq_no"
        );

        debug!(%id, %from_seq_no, %to_seq_no, "Building events by ID stream");

        let last_seq_no = self.last_seq_no(id).await?;

        let mut current_from_seq_no = from_seq_no;
        let evts = stream! {
            'outer: loop {
                let evts = self
                    .next_evts_by_id(id, current_from_seq_no, to_seq_no, evt_from_bytes)
                    .await?;

                for await evt in evts {
                    match evt {
                        Ok(evt @ (seq_no, _)) => {
                            current_from_seq_no = seq_no.succ();
                            yield Ok(evt);
                        }

                        Err(error) => {
                            yield Err(error);
                            break 'outer;
                        }
                    }
                }

                if current_from_seq_no > to_seq_no {
                    break;
                }

                // Only sleep if requesting future events.
                if (last_seq_no < Some(to_seq_no)) {
                    sleep(self.poll_interval).await;
                }
            }
        };

        Ok(evts)
    }

    async fn evts_by_tag<'a, E, EvtFromBytes, EvtFromBytesError>(
        &'a self,
        tag: String,
        from_seq_no: SeqNo,
        evt_from_bytes: EvtFromBytes,
    ) -> Result<impl Stream<Item = Result<(SeqNo, E), Self::Error>> + Send + '_, Self::Error>
    where
        E: Send + 'a,
        EvtFromBytes: Fn(Bytes) -> Result<E, EvtFromBytesError> + Copy + Send + Sync + 'static,
        EvtFromBytesError: StdError + Send + Sync + 'static,
    {
        assert!(
            from_seq_no <= Self::MAX_SEQ_NO,
            "from_seq_no must be less or equal {}",
            Self::MAX_SEQ_NO
        );

        debug!(tag, %from_seq_no, "Building events by tag stream");

        let last_seq_no = self
            .cnn()
            .await?
            .query_one("SELECT COALESCE(MAX(seq_no), 0) FROM evts", &[])
            .await
            .map_err(Error::ExecuteQuery)
            .and_then(|row| {
                (row.get::<_, i64>(0) as u64)
                    .try_into()
                    .map_err(Error::InvalidSeqNo)
            })?;

        let mut current_from_seq_no = from_seq_no;
        let evts = stream! {
            'outer: loop {
                let evts = self
                    .next_evts_by_tag(&tag, current_from_seq_no, evt_from_bytes)
                    .await?;

                for await evt in evts {
                    match evt {
                        Ok(evt @ (seq_no, _)) => {
                            current_from_seq_no = seq_no.succ();
                            yield Ok(evt);
                        }

                        Err(error) => {
                            yield Err(error);
                            break 'outer;
                        }
                    }
                }

                // Only sleep if requesting future events.
                if (current_from_seq_no >= last_seq_no) {
                    sleep(self.poll_interval).await;
                }
            }
        };

        Ok(evts)
    }
}

/// Configuration for the [PostgresEvtLog].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    host: String,
    port: u16,
    user: String,
    password: String,
    dbname: String,
    sslmode: String,
    #[serde(default = "evts_table_default")]
    evts_table: String,
    #[serde(default = "poll_interval_default", with = "humantime_serde")]
    poll_interval: Duration,
    #[serde(default = "id_broadcast_capacity_default")]
    id_broadcast_capacity: NonZeroUsize,
    #[serde(default)]
    setup: bool,
}

impl Config {
    /// Change the `host`.
    pub fn with_host<T>(self, host: T) -> Self
    where
        T: Into<String>,
    {
        let host = host.into();
        Self { host, ..self }
    }

    /// Change the `port`.
    pub fn with_port(self, port: u16) -> Self {
        Self { port, ..self }
    }

    /// Change the `user`.
    pub fn with_user<T>(self, user: T) -> Self
    where
        T: Into<String>,
    {
        let user = user.into();
        Self { user, ..self }
    }

    /// Change the `password`.
    pub fn with_password<T>(self, password: T) -> Self
    where
        T: Into<String>,
    {
        let password = password.into();
        Self { password, ..self }
    }

    /// Change the `dbname`.
    pub fn with_dbname<T>(self, dbname: T) -> Self
    where
        T: Into<String>,
    {
        let dbname = dbname.into();
        Self { dbname, ..self }
    }

    /// Change the `sslmode`.
    pub fn with_sslmode<T>(self, sslmode: T) -> Self
    where
        T: Into<String>,
    {
        let sslmode = sslmode.into();
        Self { sslmode, ..self }
    }

    /// Change the `evts_table`.
    pub fn with_evts_table(self, evts_table: String) -> Self {
        Self { evts_table, ..self }
    }

    /// Change the `poll_interval`.
    pub fn with_poll_interval(self, poll_interval: Duration) -> Self {
        Self {
            poll_interval,
            ..self
        }
    }

    /// Change the `id_broadcast_capacity`.
    pub fn with_id_broadcast_capacity(self, id_broadcast_capacity: NonZeroUsize) -> Self {
        Self {
            id_broadcast_capacity,
            ..self
        }
    }

    /// Change the `setup` flag.
    pub fn with_setup(self, setup: bool) -> Self {
        Self { setup, ..self }
    }

    fn cnn_config(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={} sslmode={}",
            self.host, self.port, self.user, self.password, self.dbname, self.sslmode
        )
    }
}

impl Default for Config {
    /// Default values suitable for local testing only.
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: "".to_string(),
            dbname: "postgres".to_string(),
            sslmode: "prefer".to_string(),
            evts_table: evts_table_default(),
            poll_interval: poll_interval_default(),
            id_broadcast_capacity: id_broadcast_capacity_default(),
            setup: false,
        }
    }
}

fn evts_table_default() -> String {
    "evts".to_string()
}

const fn poll_interval_default() -> Duration {
    Duration::from_secs(2)
}

const fn id_broadcast_capacity_default() -> NonZeroUsize {
    NonZeroUsize::MIN
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventsourced::convert;
    use std::future;
    use testcontainers::{clients::Cli, images::postgres::Postgres};

    #[tokio::test]
    async fn test_evt_log() -> Result<(), Box<dyn StdError + Send + Sync>> {
        let client = Cli::default();
        let container = client.run(Postgres::default());
        let port = container.get_host_port_ipv4(5432);

        let config = Config::default().with_port(port).with_setup(true);
        let mut evt_log = PostgresEvtLog::new(config).await?;

        let id = Uuid::now_v7();

        // Start testing.

        let last_seq_no = evt_log.last_seq_no(id).await?;
        assert_eq!(last_seq_no, None);

        evt_log
            .persist(id, &1, Some("tag".to_string()), &convert::prost::to_bytes)
            .await?;
        evt_log
            .persist(id, &2, None, &convert::prost::to_bytes)
            .await?;
        evt_log
            .persist(id, &3, Some("tag".to_string()), &convert::prost::to_bytes)
            .await?;
        let last_seq_no = evt_log.last_seq_no(id).await?;
        assert_eq!(last_seq_no, Some(3.try_into().unwrap()));

        let evts = evt_log
            .evts_by_id::<i32, _, _>(
                id,
                2.try_into().unwrap(),
                3.try_into().unwrap(),
                convert::prost::from_bytes,
            )
            .await?;
        let sum = evts
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
            .evts_by_id::<i32, _, _>(
                id,
                1.try_into().unwrap(),
                PostgresEvtLog::MAX_SEQ_NO,
                convert::prost::from_bytes,
            )
            .await?;

        let evts_by_tag = evt_log
            .evts_by_tag::<i32, _, _>("tag".to_string(), SeqNo::MIN, convert::prost::from_bytes)
            .await?;

        evt_log
            .clone()
            .persist(id, &4, None, &convert::prost::to_bytes)
            .await?;
        evt_log
            .clone()
            .persist(id, &5, Some("tag".to_string()), &convert::prost::to_bytes)
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
