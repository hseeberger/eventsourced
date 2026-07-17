//! A [SnapshotStore] implementation based on [PostgreSQL](https://www.postgresql.org/).

use crate::{Cnn, CnnPool, Error};
use bb8_postgres::{PostgresConnectionManager, bb8::Pool};
use bytes::Bytes;
use eventsourced::snapshot_store::{Snapshot, SnapshotStore};
use serde::{Deserialize, Serialize};
use std::{
    error::Error as StdError,
    fmt::{self, Debug, Formatter},
    marker::PhantomData,
    num::NonZeroU64,
};
use tokio_postgres::{NoTls, types::ToSql};
use tracing::debug;

/// A [SnapshotStore] implementation based on [PostgreSQL](https://www.postgresql.org/).
#[derive(Clone)]
pub struct PostgresSnapshotStore<I> {
    cnn_pool: CnnPool<NoTls>,
    _id: PhantomData<I>,
}

impl<I> PostgresSnapshotStore<I> {
    #[allow(missing_docs)]
    pub async fn new(config: Config) -> Result<Self, Error> {
        debug!(?config, "creating PostgresSnapshotStore");

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
                .execute(
                    &include_str!("create_snapshot_store.sql")
                        .replace("snapshots", &config.snapshots_table),
                    &[],
                )
                .await
                .map_err(|error| Error::Postgres("cannot execute query".to_string(), error))?;
        }

        Ok(Self {
            cnn_pool,
            _id: PhantomData,
        })
    }

    async fn cnn(&self) -> Result<Cnn<'_, NoTls>, Error> {
        self.cnn_pool.get().await.map_err(Error::GetConnection)
    }
}

impl<I> Debug for PostgresSnapshotStore<I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresSnapshotStore").finish()
    }
}

impl<I> SnapshotStore for PostgresSnapshotStore<I>
where
    I: Debug + Clone + ToSql + Send + Sync + 'static,
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
        self.cnn()
            .await?
            .execute(
                "INSERT INTO snapshots VALUES ($1, $2, $3)",
                &[&id, &(seq_no.get() as i64), &bytes.as_ref()],
            )
            .await
            .map_err(|error| Error::Postgres("cannot execute query".to_string(), error))
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

        self.cnn()
            .await?
            .query_opt(
                "SELECT seq_no, state FROM snapshots
                 WHERE id = $1
                 AND seq_no = (select max(seq_no) from snapshots where id = $1)",
                &[&id],
            )
            .await
            .map_err(|error| Error::Postgres("cannot execute query".to_string(), error))?
            .map(move |row| {
                let seq_no = (row.get::<_, i64>(0) as u64)
                    .try_into()
                    .map_err(|_| Error::ZeroNonZeroU64)?;
                let bytes = row.get::<_, &[u8]>(1);
                let bytes = Bytes::copy_from_slice(bytes);
                from_bytes(bytes)
                    .map_err(|source| Error::FromBytes(Box::new(source)))
                    .map(|state| Snapshot::new(seq_no, state))
            })
            .transpose()
    }
}

/// Configuration for the [PostgresSnapshotStore].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    /// The host name of the Postgres server.
    pub host: String,

    /// The port of the Postgres server.
    pub port: u16,

    /// The user name used to connect.
    pub user: String,

    /// The password used to connect.
    pub password: String,

    /// The name of the database.
    pub dbname: String,

    /// The SSL mode used to connect.
    pub sslmode: String,

    /// The name of the snapshots table.
    #[serde(default = "snapshots_table_default")]
    pub snapshots_table: String,

    /// Whether to create the database schema on startup.
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
            snapshots_table: snapshots_table_default(),
            setup: false,
        }
    }
}

fn snapshots_table_default() -> String {
    "snapshots".to_string()
}

#[cfg(test)]
mod tests {
    use crate::{PostgresSnapshotStore, PostgresSnapshotStoreConfig};
    use error_ext::BoxError;
    use eventsourced::{binarize, snapshot_store::SnapshotStore};
    use testcontainers::{ImageExt, runners::AsyncRunner};
    use testcontainers_modules::postgres::Postgres;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_snapshot_store() -> Result<(), BoxError> {
        let container = Postgres::default()
            .with_db_name("eventsourced")
            .with_user("eventsourced")
            .with_password("eventsourced")
            .with_tag("18-alpine")
            .start()
            .await?;
        let port = container.get_host_port_ipv4(5432).await?;

        let config = PostgresSnapshotStoreConfig {
            port,
            user: "eventsourced".to_string(),
            password: "eventsourced".to_string(),
            dbname: "eventsourced".to_string(),
            setup: true,
            ..Default::default()
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
