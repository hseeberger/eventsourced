//! An [EventLog] implementation based on [PostgreSQL](https://www.postgresql.org/).

use crate::pool::{self, Pool};
use async_stream::stream;
use bytes::Bytes;
use error_ext::BoxError;
use futures::{Stream, StreamExt, TryStreamExt};
use serde::Deserialize;
use sqlx::{postgres::PgRow, query::Query, Encode, Executor, IntoArguments, Postgres, Row, Type};
use std::{
    error::Error as StdError,
    fmt::{self, Debug, Formatter},
    marker::PhantomData,
    num::NonZeroU64,
    time::Duration,
};
use thiserror::Error;
use tokio::time::sleep;
use tracing::{debug, instrument};

/// An [EventLog] implementation based on [PostgreSQL](https://www.postgresql.org/).
#[derive(Clone)]
pub struct EventLog<I> {
    poll_interval: Duration,
    pool: Pool,
    _id: PhantomData<I>,
}

impl<I> EventLog<I>
where
    I: Debug + for<'q> Encode<'q, Postgres> + Type<Postgres> + Sync,
{
    #[allow(missing_docs)]
    pub async fn new(config: Config) -> Result<Self, Error> {
        debug!(?config, "creating EventLog");

        // Create connection pool.
        let pool = Pool::new(config.pool)
            .await
            .map_err(|error| Error::Sqlx("cannot create connection pool".to_string(), error))?;

        // Optionally create tables.
        if config.setup {
            let ddl = include_str!("create_event_log.sql").replace("events", &config.events_table);
            let ddl = ddl.as_str();

            (&*pool).execute(ddl).await.map_err(|error| {
                Error::Sqlx("cannot create tables for event log".to_string(), error)
            })?;
        }

        Ok(Self {
            poll_interval: config.poll_interval,
            pool,
            _id: PhantomData,
        })
    }

    #[instrument(skip(self, from_bytes))]
    async fn next_events_by_id<'i, E, FromBytes, FromBytesError>(
        &self,
        id: &'i I,
        seq_no: i64,
        from_bytes: FromBytes,
    ) -> Result<impl Stream<Item = Result<(NonZeroU64, E), Error>> + Send + 'i, Error>
    where
        E: Send,
        FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Send + 'i,
        FromBytesError: StdError + Send + Sync + 'static,
    {
        debug!(?id, ?seq_no, "querying events");

        let query = sqlx::query("SELECT seq_no, event FROM events WHERE id = $1 AND seq_no >= $2")
            .bind(id)
            .bind(seq_no);

        self.next_events(query, from_bytes).await
    }

    #[instrument(skip(self, from_bytes))]
    async fn next_events_by_type<E, FromBytes, FromBytesError>(
        &self,
        type_name: &'static str,
        seq_no: i64,
        from_bytes: FromBytes,
    ) -> Result<impl Stream<Item = Result<(NonZeroU64, E), Error>> + Send, Error>
    where
        E: Send,
        FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Send + 'static,
        FromBytesError: StdError + Send + Sync + 'static,
    {
        debug!(%type_name, seq_no, "querying events");

        let query =
            sqlx::query("SELECT seq_no, event FROM events WHERE type = $1 AND seq_no >= $2")
                .bind(type_name)
                .bind(seq_no);

        self.next_events(query, from_bytes).await
    }

    #[instrument(skip(self, query, from_bytes))]
    async fn next_events<'q, A, E, FromBytes, FromBytesError>(
        &self,
        query: Query<'q, Postgres, A>,
        from_bytes: FromBytes,
    ) -> Result<impl Stream<Item = Result<(NonZeroU64, E), Error>> + Send + 'q, Error>
    where
        A: Send + IntoArguments<'q, Postgres> + 'q,
        E: Send,
        FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Send + 'q,
        FromBytesError: StdError + Send + Sync + 'static,
    {
        let events = query
            .fetch(&*self.pool)
            .map_err(|error| Error::Sqlx("cannot get next row".to_string(), error))
            .map(move |row| {
                row.and_then(|row| {
                    let seq_no = (row.get::<i64, _>(0) as u64)
                        .try_into()
                        .map_err(|_| Error::ZeroSeqNo)?;
                    let bytes = row.get::<&[u8], _>(1);
                    let bytes = Bytes::copy_from_slice(bytes);
                    from_bytes(bytes)
                        .map_err(|source| Error::FromBytes(Box::new(source)))
                        .map(|event| (seq_no, event))
                })
            });

        Ok(events)
    }

    #[instrument(skip(self))]
    async fn last_seq_no_by_type(
        &self,
        type_name: &'static str,
    ) -> Result<Option<NonZeroU64>, Error> {
        sqlx::query("SELECT MAX(seq_no) FROM events WHERE type = $1")
            .bind(type_name)
            .fetch_one(&*self.pool)
            .await
            .map_err(|error| Error::Sqlx("cannot select max seq_no".to_string(), error))
            .and_then(into_seq_no)
    }
}

impl<I> Debug for EventLog<I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventLog").finish()
    }
}

impl<I> eventsourced::event_log::EventLog for EventLog<I>
where
    I: Debug + Clone + for<'q> Encode<'q, Postgres> + Type<Postgres> + Send + Sync + 'static,
{
    type Id = I;
    type Error = Error;

    /// The maximum value for sequence numbers. As PostgreSQL does not support unsigned integers,
    /// this is `i64::MAX` or `9_223_372_036_854_775_807`.
    const MAX_SEQ_NO: NonZeroU64 = unsafe { NonZeroU64::new_unchecked(i64::MAX as u64) };

    #[instrument(skip(self, events, to_bytes))]
    async fn persist<E, ToBytes, ToBytesError>(
        &mut self,
        type_name: &'static str,
        id: &Self::Id,
        last_seq_no: Option<NonZeroU64>,
        events: &[E],
        to_bytes: &ToBytes,
    ) -> Result<NonZeroU64, Self::Error>
    where
        ToBytes: Fn(&E) -> Result<Bytes, ToBytesError> + Sync,
        ToBytesError: StdError + Send + Sync + 'static,
    {
        if events.is_empty() {
            return Err(Error::EmptyEvents);
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| Error::Sqlx("cannot begin transaction".to_string(), error))?;

        let seq_no = sqlx::query("SELECT MAX(seq_no) FROM events WHERE id = $1")
            .bind(id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|error| Error::Sqlx("cannot select max seq_no".to_string(), error))
            .and_then(into_seq_no)?;

        if seq_no != last_seq_no {
            return Err(Error::UnexpectedSeqNo(seq_no, last_seq_no));
        }

        let mut seq_no = last_seq_no.map(|n| n.get() as i64).unwrap_or_default();
        for event in events.iter() {
            seq_no += 1;
            let bytes = to_bytes(event).map_err(|error| Error::ToBytes(error.into()))?;
            sqlx::query("INSERT INTO events VALUES ($1, $2, $3, $4)")
                .bind(type_name)
                .bind(id)
                .bind(seq_no)
                .bind(bytes.as_ref())
                .execute(&mut *tx)
                .await
                .map_err(|error| Error::Sqlx("cannot execute statement".to_string(), error))?;
        }

        tx.commit()
            .await
            .map_err(|error| Error::Sqlx("cannot commit transaction".to_string(), error))?;

        (seq_no as u64).try_into().map_err(|_| Error::ZeroSeqNo)
    }

    #[instrument(skip(self))]
    async fn last_seq_no(
        &self,
        _type_name: &'static str,
        id: &Self::Id,
    ) -> Result<Option<NonZeroU64>, Self::Error> {
        sqlx::query("SELECT MAX(seq_no) FROM events WHERE id = $1")
            .bind(id)
            .fetch_one(&*self.pool)
            .await
            .map_err(|error| Error::Sqlx("cannot select max seq_no".to_string(), error))
            .and_then(into_seq_no)
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

fn into_seq_no(row: PgRow) -> Result<Option<NonZeroU64>, Error> {
    // If there is no seq_no there is one row with a NULL column, hence use `try_get`.
    row.try_get::<i64, _>(0)
        .ok()
        .map(|seq_no| (seq_no as u64).try_into().map_err(|_| Error::ZeroSeqNo))
        .transpose()
}

/// Configuration for the [EventLog].
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub pool: pool::Config,

    #[serde(default = "events_table_default")]
    pub events_table: String,

    #[serde(default = "poll_interval_default", with = "humantime_serde")]
    pub poll_interval: Duration,

    #[serde(default)]
    pub setup: bool,
}

/// Error for the [EventLog].
#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Sqlx(String, #[source] sqlx::Error),

    #[error("cannot convert event to bytes")]
    ToBytes(#[source] BoxError),

    #[error("cannot convert bytes to event")]
    FromBytes(#[source] BoxError),

    #[error("expected sequence number {0:?}, but was {1:?}")]
    UnexpectedSeqNo(Option<NonZeroU64>, Option<NonZeroU64>),

    #[error("sequence number must not be zero")]
    ZeroSeqNo,

    #[error("events to be persisted must not be empty")]
    EmptyEvents,
}

fn events_table_default() -> String {
    "events".to_string()
}

const fn poll_interval_default() -> Duration {
    Duration::from_secs(2)
}

#[cfg(test)]
mod tests {
    use crate::event_log::{events_table_default, poll_interval_default, Config, EventLog};
    use error_ext::BoxError;
    use eventsourced::{binarize, event_log::EventLog as _EventLog};
    use futures::{StreamExt, TryStreamExt};
    use sqlx::postgres::PgSslMode;
    use std::{future, num::NonZeroU64};
    use testcontainers::clients::Cli;
    use testcontainers_modules::postgres::Postgres;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_event_log() -> Result<(), BoxError> {
        let client = Cli::default();
        let container = client.run(Postgres::default().with_host_auth());
        let port = container.get_host_port_ipv4(5432);

        let pool = crate::pool::Config {
            host: "localhost".to_string(),
            port,
            user: "postgres".to_string(),
            password: "".to_string().into(),
            dbname: "postgres".to_string(),
            sslmode: PgSslMode::Prefer,
        };
        let config = Config {
            pool,
            setup: true,
            events_table: events_table_default(),
            poll_interval: poll_interval_default(),
        };
        let mut event_log = EventLog::<Uuid>::new(config).await?;

        let id = Uuid::now_v7();

        // Start testing.

        let last_seq_no = event_log.last_seq_no("counter", &id).await?;
        assert_eq!(last_seq_no, None);

        let last_seq_no = event_log
            .persist("counter", &id, None, &[1], &binarize::serde_json::to_bytes)
            .await?;
        assert!(last_seq_no.get() == 1);

        event_log
            .persist(
                "counter",
                &id,
                Some(last_seq_no),
                &[2],
                &binarize::serde_json::to_bytes,
            )
            .await?;

        let result = event_log
            .persist(
                "counter",
                &id,
                Some(last_seq_no),
                &[3],
                &binarize::serde_json::to_bytes,
            )
            .await;
        assert!(result.is_err());

        event_log
            .persist(
                "counter",
                &id,
                Some(last_seq_no.checked_add(1).expect("overflow")),
                &[3],
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
                &[4, 5],
                &binarize::serde_json::to_bytes,
            )
            .await?;
        event_log
            .clone()
            .persist(
                "counter",
                &id,
                Some(last_seq_no),
                &[6, 7],
                &binarize::serde_json::to_bytes,
            )
            .await?;
        let last_seq_no = event_log.last_seq_no("counter", &id).await?;
        assert_eq!(last_seq_no, Some(7.try_into()?));

        let sum = events
            .take(7)
            .try_fold(0u32, |acc, (_, n)| future::ready(Ok(acc + n)))
            .await?;
        assert_eq!(sum, 28);

        Ok(())
    }
}
