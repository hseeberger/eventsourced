//! An [EventLog] implementation based on [PostgreSQL](https://www.postgresql.org/).

use crate::{Cnn, CnnPool, Error};
use async_stream::stream;
use bb8_postgres::{bb8::Pool, PostgresConnectionManager};
use bytes::Bytes;
use eventsourced::event_log::EventLog;
use futures::{Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::{
    error::Error as StdError,
    fmt::{self, Debug, Formatter},
    marker::PhantomData,
    num::{NonZeroU64, NonZeroUsize},
    time::Duration,
};
use tokio::time::sleep;
use tokio_postgres::{types::ToSql, NoTls};
use tracing::{debug, instrument};

/// An [EventLog] implementation based on [PostgreSQL](https://www.postgresql.org/).
#[derive(Clone)]
pub struct PostgresEventLog<I> {
    poll_interval: Duration,
    cnn_pool: CnnPool<NoTls>,
    _id: PhantomData<I>,
}

impl<I> PostgresEventLog<I>
where
    I: ToSql + Sync,
{
    #[allow(missing_docs)]
    pub async fn new(config: Config) -> Result<Self, Error> {
        debug!(?config, "creating PostgresEventLog");

        // Create connection pool.
        let tls = NoTls;
        let cnn_manager = PostgresConnectionManager::new_from_stringlike(config.cnn_config(), tls)
            .map_err(|error| {
                Error::Postgres("cannot create connection manager".to_string(), error)
            })?;
        let cnn_pool = Pool::builder()
            .build(cnn_manager)
            .await
            .map_err(|error| Error::Postgres("cannot create connection pool".to_string(), error))?;

        // Setup tables.
        if config.setup {
            cnn_pool
                .get()
                .await
                .map_err(Error::GetConnection)?
                .batch_execute(
                    &include_str!("create_event_log.sql").replace("events", &config.events_table),
                )
                .await
                .map_err(|error| Error::Postgres("cannot execute query".to_string(), error))?;
        }

        Ok(Self {
            poll_interval: config.poll_interval,
            cnn_pool,
            _id: PhantomData,
        })
    }

    async fn cnn(&self) -> Result<Cnn<NoTls>, Error> {
        self.cnn_pool.get().await.map_err(Error::GetConnection)
    }

    async fn next_events_by_id<E, FromBytes, FromBytesError>(
        &self,
        id: &I,
        seq_no: i64,
        from_bytes: FromBytes,
    ) -> Result<impl Stream<Item = Result<(NonZeroU64, E), Error>> + Send, Error>
    where
        E: Send,
        FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Send,
        FromBytesError: StdError + Send + Sync + 'static,
    {
        debug!(?id, ?seq_no, "querying events");
        let params: [&(dyn ToSql + Sync); 2] = [&id, &seq_no];
        let events = self
            .cnn()
            .await?
            .query_raw(
                "SELECT seq_no, event FROM events WHERE id = $1 AND seq_no >= $2",
                params,
            )
            .await
            .map_err(|error| Error::Postgres("cannot execute query".to_string(), error))?
            .map_err(|error| Error::Postgres("cannot get next row".to_string(), error))
            .map(move |row| {
                row.and_then(|row| {
                    let seq_no = (row.get::<_, i64>(0) as u64)
                        .try_into()
                        .map_err(|_| Error::ZeroNonZeroU64)?;
                    let bytes = row.get::<_, &[u8]>(1);
                    let bytes = Bytes::copy_from_slice(bytes);
                    from_bytes(bytes)
                        .map_err(|source| Error::FromBytes(Box::new(source)))
                        .map(|event| (seq_no, event))
                })
            });

        Ok(events)
    }

    async fn next_events_by_type<E, FromBytes, FromBytesError>(
        &self,
        type_name: &str,
        seq_no: i64,
        from_bytes: FromBytes,
    ) -> Result<impl Stream<Item = Result<(NonZeroU64, E), Error>> + Send, Error>
    where
        E: Send,
        FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Send,
        FromBytesError: StdError + Send + Sync + 'static,
    {
        debug!(%type_name, seq_no, "querying events");

        let params: [&(dyn ToSql + Sync); 2] = [&type_name, &seq_no];
        let events = self
            .cnn()
            .await?
            .query_raw(
                "SELECT seq_no, event FROM events WHERE type = $1 AND seq_no >= $2",
                params,
            )
            .await
            .map_err(|error| Error::Postgres("cannot execute query".to_string(), error))?
            .map_err(|error| Error::Postgres("cannot get next row".to_string(), error))
            .map(move |row| {
                row.and_then(|row| {
                    let seq_no = (row.get::<_, i64>(0) as u64)
                        .try_into()
                        .map_err(|_| Error::ZeroNonZeroU64)?;
                    let bytes = row.get::<_, &[u8]>(1);
                    let bytes = Bytes::copy_from_slice(bytes);
                    from_bytes(bytes)
                        .map_err(|source| Error::FromBytes(Box::new(source)))
                        .map(|event| (seq_no, event))
                })
            });

        Ok(events)
    }

    async fn last_seq_no_by_type(&self, type_name: &str) -> Result<Option<NonZeroU64>, Error> {
        self.cnn()
            .await?
            .query_one(
                "SELECT MAX(seq_no) FROM events WHERE type = $1",
                &[&type_name],
            )
            .await
            .map_err(|error| Error::Postgres("cannot execute query".to_string(), error))
            .and_then(|row| {
                // If there is no seq_no there is one row with a NULL column, hence use `try_get`.
                row.try_get::<_, i64>(0)
                    .ok()
                    .map(|seq_no| {
                        (seq_no as u64)
                            .try_into()
                            .map_err(|_| Error::ZeroNonZeroU64)
                    })
                    .transpose()
            })
    }
}

impl<I> Debug for PostgresEventLog<I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresEventLog").finish()
    }
}

impl<I> EventLog for PostgresEventLog<I>
where
    I: Clone + ToSql + Send + Sync + 'static,
{
    type Id = I;

    type Error = Error;

    /// The maximum value for sequence numbers. As PostgreSQL does not support unsigned integers,
    /// this is `i64::MAX` or `9_223_372_036_854_775_807`.
    const MAX_SEQ_NO: NonZeroU64 = unsafe { NonZeroU64::new_unchecked(i64::MAX as u64) };

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
        let seq_no = last_seq_no.map(|n| n.get() as i64).unwrap_or_default() + 1;

        let bytes = to_bytes(event).map_err(|error| Error::ToBytes(Box::new(error)))?;

        self.cnn()
            .await?
            .query_one(
                "INSERT INTO events (seq_no, type, id, event) VALUES ($1, $2, $3, $4) RETURNING seq_no",
                &[&seq_no, &type_name, &id, &bytes.as_ref()],
            )
            .await
            .map_err(|error| Error::Postgres("cannot execute query".to_string(), error))
            .and_then(|row| {
                (row.get::<_, i64>(0) as u64)
                    .try_into()
                    .map_err(|_| Error::ZeroNonZeroU64)
            })
    }

    #[instrument(skip(self))]
    async fn last_seq_no(
        &self,
        type_name: &'static str,
        id: &Self::Id,
    ) -> Result<Option<NonZeroU64>, Self::Error> {
        self.cnn()
            .await?
            .query_one("SELECT MAX(seq_no) FROM events WHERE id = $1", &[&id])
            .await
            .map_err(|error| Error::Postgres("cannot execute query".to_string(), error))
            .and_then(|row| {
                // If there is no seq_no there is one row with a NULL column, hence use `try_get`.
                row.try_get::<_, i64>(0)
                    .ok()
                    .map(|seq_no| {
                        (seq_no as u64)
                            .try_into()
                            .map_err(|_| Error::ZeroNonZeroU64)
                    })
                    .transpose()
            })
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
        let last_seq_no = self
            .last_seq_no(type_name, id)
            .await?
            .map(|n| n.get() as i64)
            .unwrap_or_default();

        let mut current_seq_no = seq_no.get() as i64;
        let events = stream! {
            'outer: loop {
                let events = self
                    .next_events_by_id(id, current_seq_no, from_bytes)
                    .await?;

                for await event in events {
                    match event {
                        Ok(event @ (seq_no, _)) => {
                            current_seq_no += seq_no.get() as i64 + 1;
                            yield Ok(event);
                        }

                        Err(error) => {
                            yield Err(error);
                            break 'outer;
                        }
                    }
                }

                // Only sleep if requesting future events.
                if current_seq_no >= last_seq_no {
                    sleep(self.poll_interval).await;
                }
            }
        };

        Ok(events)
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

        let last_seq_no = self
            .last_seq_no_by_type(type_name)
            .await?
            .map(|n| n.get() as i64)
            .unwrap_or_default();

        let mut current_seq_no = seq_no.get() as i64;
        let events = stream! {
            'outer: loop {
                let events = self
                    .next_events_by_type(type_name, current_seq_no, from_bytes)
                    .await?;

                for await event in events {
                    match event {
                        Ok(event @ (seq_no, _)) => {
                            current_seq_no = seq_no.get() as i64 + 1;
                            yield Ok(event);
                        }

                        Err(error) => {
                            yield Err(error);
                            break 'outer;
                        }
                    }
                }

                // Only sleep if requesting future events.
                if current_seq_no >= last_seq_no {
                    sleep(self.poll_interval).await;
                }
            }
        };

        Ok(events)
    }
}

/// Configuration for the [PostgresEventLog].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub host: String,

    pub port: u16,

    pub user: String,

    pub password: String,

    pub dbname: String,

    pub sslmode: String,

    #[serde(default = "events_table_default")]
    pub events_table: String,

    #[serde(default = "poll_interval_default", with = "humantime_serde")]
    pub poll_interval: Duration,

    #[serde(default = "id_broadcast_capacity_default")]
    pub id_broadcast_capacity: NonZeroUsize,

    #[serde(default)]
    pub setup: bool,
}

impl Config {
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
            events_table: events_table_default(),
            poll_interval: poll_interval_default(),
            id_broadcast_capacity: id_broadcast_capacity_default(),
            setup: false,
        }
    }
}

fn events_table_default() -> String {
    "events".to_string()
}

const fn poll_interval_default() -> Duration {
    Duration::from_secs(2)
}

const fn id_broadcast_capacity_default() -> NonZeroUsize {
    NonZeroUsize::MIN
}

#[cfg(test)]
mod tests {
    use crate::{PostgresEventLog, PostgresEventLogConfig};
    use error_ext::BoxError;
    use eventsourced::{binarize, event_log::EventLog};
    use futures::{StreamExt, TryStreamExt};
    use std::{future, num::NonZeroU64};
    use testcontainers::clients::Cli;
    use testcontainers_modules::postgres::Postgres;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_event_log() -> Result<(), BoxError> {
        let client = Cli::default();
        let container = client.run(Postgres::default().with_host_auth());
        let port = container.get_host_port_ipv4(5432);

        let config = PostgresEventLogConfig {
            port,
            setup: true,
            ..Default::default()
        };
        let mut event_log = PostgresEventLog::<Uuid>::new(config).await?;

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
