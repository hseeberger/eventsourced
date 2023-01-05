//! A [SnapshotStore] implementation based on [PostgreSQL](https://www.postgresql.org/).

use crate::{Cnn, CnnPool, Error};
use bb8_postgres::{bb8::Pool, PostgresConnectionManager};
use bytes::Bytes;
use eventsourced::{Metadata, Snapshot, SnapshotStore};
use serde::{Deserialize, Serialize};
use std::{
    error::Error as StdError,
    fmt::{self, Debug, Formatter},
};
use tokio_postgres::NoTls;
use tracing::debug;
use uuid::Uuid;

#[derive(Clone)]
pub struct PostgresSnapshotStore {
    cnn_pool: CnnPool<NoTls>,
}

impl PostgresSnapshotStore {
    #[allow(missing_docs)]
    pub async fn new(config: Config) -> Result<Self, Error> {
        debug!(?config, "Creating PostgresSnapshotStore");

        // Create connection pool.
        let tls = NoTls;
        let cnn_manager = PostgresConnectionManager::new_from_stringlike(config.cnn_config(), tls)
            .map_err(Error::ConnectionManager)?;
        let cnn_pool = Pool::builder()
            .build(cnn_manager)
            .await
            .map_err(Error::ConnectionPool)?;

        Ok(Self { cnn_pool })
    }

    pub async fn setup(&self) -> Result<(), Box<dyn StdError + Send + Sync>> {
        self.cnn()
            .await?
            .execute(include_str!("create_snapshot_store.sql"), &[])
            .await?;
        Ok(())
    }

    async fn cnn(&self) -> Result<Cnn<NoTls>, Error> {
        self.cnn_pool.get().await.map_err(Error::GetConnection)
    }
}

impl Debug for PostgresSnapshotStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresSnapshotStore").finish()
    }
}

impl SnapshotStore for PostgresSnapshotStore {
    type Error = Error;

    async fn save<'a, 'b, 'c, S, StateToBytes, StateToBytesError>(
        &'a mut self,
        id: Uuid,
        seq_no: u64,
        state: &'b S,
        _metadata: Metadata,
        state_to_bytes: &'c StateToBytes,
    ) -> Result<(), Self::Error>
    where
        'b: 'a,
        'c: 'a,
        S: Send + Sync + 'a,
        StateToBytes: Fn(&S) -> Result<Bytes, StateToBytesError> + Send + Sync + 'static,
        StateToBytesError: StdError + Send + Sync + 'static,
    {
        let cnn = self.cnn().await?;
        let bytes = state_to_bytes(state).map_err(|source| Error::ToBytes(Box::new(source)))?;
        cnn.execute(
            "INSERT INTO snapshots VALUES ($1, $2, $3)",
            &[&id, &(seq_no as i64), &bytes.as_ref()],
        )
        .await
        .map_err(Error::ExecuteStmt)?;
        Ok(())
    }

    async fn load<'a, 'b, S, StateFromBytes, StateFromBytesError>(
        &'a self,
        id: Uuid,
        state_from_bytes: &'b StateFromBytes,
    ) -> Result<Option<Snapshot<S>>, Self::Error>
    where
        'b: 'a,
        S: 'a,
        StateFromBytes: Fn(Bytes) -> Result<S, StateFromBytesError> + Send + Sync + 'static,
        StateFromBytesError: StdError + Send + Sync + 'static,
    {
        let cnn = self.cnn().await?;
        cnn.query_opt("SELECT seq_no, state FROM snapshots WHERE id = $1", &[&id])
            .await
            .map_err(Error::ExecuteStmt)?
            .map(move |row| {
                let seq_no = row.get::<_, i64>(0) as u64;
                let bytes = row.get::<_, &[u8]>(1);
                let bytes = Bytes::copy_from_slice(bytes);
                state_from_bytes(bytes)
                    .map_err(|source| Error::FromBytes(Box::new(source)))
                    .map(|state| Snapshot::new(seq_no, state, None))
            })
            .transpose()
    }
}

/// Configuration for the [PostgresSnapshotStore].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    host: String,
    port: u16,
    user: String,
    password: String,
    dbname: String,
    sslmode: String,
}

impl Config {
    pub fn with_host<T>(self, host: T) -> Self
    where
        T: Into<String>,
    {
        let host = host.into();
        Self { host, ..self }
    }

    pub fn with_port(self, port: u16) -> Self {
        Self { port, ..self }
    }

    pub fn with_user<T>(self, user: T) -> Self
    where
        T: Into<String>,
    {
        let user = user.into();
        Self { user, ..self }
    }

    pub fn with_password<T>(self, password: T) -> Self
    where
        T: Into<String>,
    {
        let password = password.into();
        Self { password, ..self }
    }

    pub fn with_dbname<T>(self, dbname: T) -> Self
    where
        T: Into<String>,
    {
        let dbname = dbname.into();
        Self { dbname, ..self }
    }

    pub fn with_sslmode<T>(self, sslmode: T) -> Self
    where
        T: Into<String>,
    {
        let sslmode = sslmode.into();
        Self { sslmode, ..self }
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventsourced::convert;
    use testcontainers::{clients::Cli, images::postgres::Postgres};

    #[tokio::test]
    async fn test() -> Result<(), Box<dyn StdError + Send + Sync>> {
        let client = Cli::default();
        let container = client.run(Postgres::default());
        let port = container.get_host_port_ipv4(5432);

        let config = Config::default().with_port(port);
        let mut snapshot_store = PostgresSnapshotStore::new(config).await?;
        snapshot_store.setup().await?;

        let id = Uuid::now_v7();

        let snapshot = snapshot_store
            .load::<i32, _, _>(id, &convert::prost::from_bytes)
            .await?;
        assert!(snapshot.is_none());

        let seq_no = 42;
        let state = 666;

        snapshot_store
            .save(id, seq_no, &state, None, &convert::prost::to_bytes)
            .await?;

        let snapshot = snapshot_store
            .load::<i32, _, _>(id, &convert::prost::from_bytes)
            .await?;
        assert!(snapshot.is_some());
        let snapshot = snapshot.unwrap();
        assert_eq!(snapshot.seq_no, seq_no);
        assert_eq!(snapshot.state, state);
        assert!(snapshot.metadata.is_none());

        Ok(())
    }
}
