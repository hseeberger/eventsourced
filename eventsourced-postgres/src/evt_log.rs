//! An [EvtLog] implementation based on [PostgreSQL](https://www.postgresql.org/).

use crate::{Cnn, CnnPool, Error};
use async_stream::stream;
use bb8_postgres::{bb8::Pool, PostgresConnectionManager};
use bytes::Bytes;
use eventsourced::{EvtLog, Metadata};
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
                .execute(
                    &include_str!("create_evt_log.sql").replace("evts", &config.evts_table),
                    &[],
                )
                .await
                .map_err(Error::ExecuteStmt)?;
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
        from_seq_no: u64,
        to_seq_no: u64,
        _metadata: Metadata,
        evt_from_bytes: EvtFromBytes,
    ) -> Result<impl Stream<Item = Result<(u64, E), Error>> + Send, Error>
    where
        E: Send,
        EvtFromBytes: Fn(Bytes) -> Result<E, EvtFromBytesError> + Copy + Send + Sync + 'static,
        EvtFromBytesError: StdError + Send + Sync + 'static,
    {
        debug!(%id, from_seq_no, to_seq_no, "Querying events");

        let params: [&(dyn ToSql + Sync); 3] = [&id, &(from_seq_no as i64), &(to_seq_no as i64)];
        let evts = self
            .cnn()
            .await?
            .query_raw(
                "SELECT seq_no, evt FROM evts WHERE id = $1 AND seq_no >= $2 AND seq_no <= $3",
                params,
            )
            .await
            .map_err(Error::ExecuteStmt)?
            .map_err(Error::NextRow)
            .map(move |row| {
                row.and_then(|row| {
                    let seq_no = row.get::<_, i64>(0) as u64;
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
    const MAX_SEQ_NO: u64 = i64::MAX as u64;

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

        debug!(%id, last_seq_no, len = evts.len(), "Persisting events");

        // Persist all events transactionally.
        let mut cnn = self.cnn().await?;
        let tx = cnn.transaction().await.map_err(Error::StartTx)?;
        let stmt = tx
            .prepare("INSERT INTO evts VALUES ($1, $2, $3)")
            .await
            .map_err(Error::PrepareStmt)?;
        for (n, evt) in evts.iter().enumerate() {
            let seq_no = (last_seq_no + 1 + n as u64) as i64;
            let bytes = evt_to_bytes(evt).map_err(|source| Error::ToBytes(Box::new(source)))?;
            tx.execute(&stmt, &[&id, &seq_no, &bytes.as_ref()])
                .await
                .map_err(Error::ExecuteStmt)?;
        }
        tx.commit().await.map_err(Error::CommitTx)?;

        Ok(None)
    }

    async fn last_seq_no(&self, id: Uuid) -> Result<u64, Self::Error> {
        let last_seq_no = self
            .cnn()
            .await?
            .query_opt(
                "SELECT COALESCE(MAX(seq_no), 0) FROM evts WHERE id = $1",
                &[&id],
            )
            .await
            .map_err(Error::ExecuteStmt)?
            .map(|row| row.get::<_, i64>(0) as u64)
            .unwrap_or_default();
        Ok(last_seq_no)
    }

    async fn evts_by_id<'a, E, EvtFromBytes, EvtFromBytesError>(
        &'a self,
        id: Uuid,
        from_seq_no: NonZeroU64,
        to_seq_no: u64,
        _metadata: Metadata,
        evt_from_bytes: EvtFromBytes,
    ) -> Result<impl Stream<Item = Result<(u64, E), Self::Error>> + Send + '_, Self::Error>
    where
        E: Debug + Send + 'a,
        EvtFromBytes: Fn(Bytes) -> Result<E, EvtFromBytesError> + Copy + Send + Sync + 'static,
        EvtFromBytesError: StdError + Send + Sync + 'static,
    {
        assert!(
            from_seq_no.get() <= to_seq_no,
            "from_seq_no must be less than or equal to to_seq_no"
        );
        assert!(
            to_seq_no <= Self::MAX_SEQ_NO,
            "to_seq_no must be less or equal {}",
            Self::MAX_SEQ_NO
        );

        debug!(%id, from_seq_no, to_seq_no, "Building event stream");

        let last_seq_no = self.last_seq_no(id).await?;

        let mut current_from_seq_no = from_seq_no.get();
        let evts = stream! {
            'outer: loop {
                let evts = self
                    .next_evts_by_id(id, current_from_seq_no, to_seq_no, None, evt_from_bytes)
                    .await?;

                for await evt in evts {
                    match evt {
                        Ok(evt @ (seq_no, _)) => {
                            current_from_seq_no = seq_no + 1;
                            yield Ok(evt);
                        }
                        err => {
                            yield err;
                            break 'outer;
                        }
                    }
                }

                if current_from_seq_no > to_seq_no {
                    break;
                }

                // Only sleep if requesting future events.
                if (last_seq_no < to_seq_no) {
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
        assert_eq!(last_seq_no, 0);

        evt_log
            .persist(id, [1, 2, 3].as_ref(), 0, &convert::prost::to_bytes)
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
                PostgresEvtLog::MAX_SEQ_NO,
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

        Ok(())
    }
}
