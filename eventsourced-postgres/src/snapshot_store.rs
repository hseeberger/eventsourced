//! A [SnapshotStore] implementation based on [PostgreSQL](https://www.postgresql.org/).

use crate::pool::{self, Pool};
use bytes::Bytes;
use error_ext::BoxError;
use eventsourced::snapshot_store::{Snapshot, SnapshotStore};
use serde::Deserialize;
use sqlx::{Encode, Executor, Postgres, Row, Type};
use std::{
    error::Error as StdError,
    fmt::{self, Debug, Formatter},
    marker::PhantomData,
    num::NonZeroU64,
};
use thiserror::Error;
use tracing::debug;

/// A [SnapshotStore] implementation based on [PostgreSQL](https://www.postgresql.org/).
#[derive(Clone)]
pub struct PostgresSnapshotStore<I> {
    pool: Pool,
    _id: PhantomData<I>,
}

impl<I> PostgresSnapshotStore<I> {
    #[allow(missing_docs)]
    pub async fn new(config: Config) -> Result<Self, Error> {
        debug!(?config, "creating PostgresSnapshotStore");

        // Create connection pool.
        let pool = Pool::new(config.pool)
            .await
            .map_err(|error| Error::Sqlx("cannot create connection pool".to_string(), error))?;

        // Optionally create tables.
        if config.setup {
            let ddl = include_str!("create_snapshot_store.sql")
                .replace("snapshots", &config.snapshots_table);
            let ddl = ddl.as_str();

            (&*pool).execute(ddl).await.map_err(|error| {
                Error::Sqlx("cannot create tables for event log".to_string(), error)
            })?;
        }

        Ok(Self {
            pool,
            _id: PhantomData,
        })
    }
}

impl<I> Debug for PostgresSnapshotStore<I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresSnapshotStore").finish()
    }
}

impl<I> SnapshotStore for PostgresSnapshotStore<I>
where
    I: Debug + Clone + for<'q> Encode<'q, Postgres> + Type<Postgres> + Send + Sync + 'static,
{
    type Id = I;

    type Error = Error;

    async fn save<S, ToBytes, ToBytesError>(
        &mut self,
        id: &Self::Id,
        seq_no: NonZeroU64,
        state: &S,
        to_bytes: &ToBytes,
    ) -> Result<(), Self::Error>
    where
        S: Send,
        ToBytes: Fn(&S) -> Result<Bytes, ToBytesError> + Sync,
        ToBytesError: StdError + Send + Sync + 'static,
    {
        debug!(?id, %seq_no, "saving snapshot");

        let bytes = to_bytes(state).map_err(|source| Error::ToBytes(Box::new(source)))?;
        sqlx::query("INSERT INTO snapshots VALUES ($1, $2, $3)")
            .bind(id)
            .bind(seq_no.get() as i64)
            .bind(bytes.as_ref())
            .execute(&*self.pool)
            .await
            .map_err(|error| Error::Sqlx("cannot insert snapshot".to_string(), error))
            .map(|_| ())
    }

    async fn load<S, FromBytes, FromBytesError>(
        &self,
        id: &Self::Id,
        from_bytes: FromBytes,
    ) -> Result<Option<Snapshot<S>>, Self::Error>
    where
        FromBytes: Fn(Bytes) -> Result<S, FromBytesError> + Send,
        FromBytesError: StdError + Send + Sync + 'static,
    {
        debug!(?id, "loading snapshot");

        sqlx::query(
            "SELECT seq_no, state FROM snapshots
             WHERE id = $1
             AND seq_no = (select max(seq_no) from snapshots where id = $1)",
        )
        .bind(id)
        .fetch_optional(&*self.pool)
        .await
        .map_err(|error| Error::Sqlx("cannot query snapshot".to_string(), error))?
        .map(move |row| {
            let seq_no = (row.get::<i64, _>(0) as u64)
                .try_into()
                .map_err(|_| Error::ZeroSeqNo)?;
            let bytes = row.get::<&[u8], _>(1);
            let bytes = Bytes::copy_from_slice(bytes);
            from_bytes(bytes)
                .map_err(|source| Error::FromBytes(Box::new(source)))
                .map(|state| Snapshot::new(seq_no, state))
        })
        .transpose()
    }
}

/// Configuration for the [PostgresSnapshotStore].
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub pool: pool::Config,

    #[serde(default = "snapshots_table_default")]
    pub snapshots_table: String,

    #[serde(default)]
    pub setup: bool,
}

/// Errors from the [PostgresEventLog] or [PostgresSnapshotStore].
#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Sqlx(String, #[source] sqlx::Error),

    #[error("cannot convert snapshot to bytes")]
    ToBytes(#[source] BoxError),

    #[error("cannot convert bytes to snapshot")]
    FromBytes(#[source] BoxError),

    #[error("sequence number must not be zero")]
    ZeroSeqNo,
}

fn snapshots_table_default() -> String {
    "snapshots".to_string()
}

#[cfg(test)]
mod tests {
    use crate::{
        snapshot_store::snapshots_table_default, PostgresSnapshotStore, PostgresSnapshotStoreConfig,
    };
    use error_ext::BoxError;
    use eventsourced::{binarize, snapshot_store::SnapshotStore};
    use sqlx::postgres::PgSslMode;
    use testcontainers::clients::Cli;
    use testcontainers_modules::postgres::Postgres;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_snapshot_store() -> Result<(), BoxError> {
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
        let config = PostgresSnapshotStoreConfig {
            pool,
            setup: true,
            snapshots_table: snapshots_table_default(),
        };
        let mut snapshot_store = PostgresSnapshotStore::<Uuid>::new(config).await?;

        let id = Uuid::now_v7();

        let snapshot = snapshot_store
            .load::<i32, _, _>(&id, &binarize::serde_json::from_bytes)
            .await?;
        assert!(snapshot.is_none());

        let seq_no = 42.try_into().unwrap();
        let state = 666;

        snapshot_store
            .save(&id, seq_no, &state, &binarize::serde_json::to_bytes)
            .await?;

        let snapshot = snapshot_store
            .load::<i32, _, _>(&id, &binarize::serde_json::from_bytes)
            .await?;

        assert!(snapshot.is_some());
        let snapshot = snapshot.unwrap();
        assert_eq!(snapshot.seq_no, seq_no);
        assert_eq!(snapshot.state, state);

        Ok(())
    }
}
