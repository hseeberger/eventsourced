//! An [EvtLog] implementation based on [PostgreSQL](https://www.postgresql.org/).

use async_stream::stream;
use bb8_postgres::{
    bb8::{Pool, PooledConnection},
    PostgresConnectionManager,
};
use bytes::Bytes;
use eventsourced::{EvtLog, Metadata};
use futures::{Stream, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::{
    convert::identity,
    fmt::{self, Debug, Formatter},
};
use thiserror::Error;
use tokio_postgres::{types::ToSql, NoTls};
use tracing::debug;
use uuid::Uuid;

/// An [EvtLog] implementation based on [PostgreSQL](https://www.postgresql.org/).
#[derive(Clone)]
pub struct PostgresEvtLog {
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

        Ok(Self { cnn_pool })
    }

    async fn cnn(&self) -> Result<Cnn<NoTls>, Error> {
        self.cnn_pool.get().await.map_err(Error::GetConnection)
    }

    pub async fn setup(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.cnn()
            .await?
            .execute(include_str!("create_tables.sql"), &[])
            .await?;
        Ok(())
    }
}

impl Debug for PostgresEvtLog {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresEvtLog").finish()
    }
}

impl EvtLog for PostgresEvtLog {
    type Error = Error;

    async fn persist<'a, 'b, 'c, E, EvtToBytes, EvtToBytesError>(
        &'a mut self,
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
        EvtToBytesError: std::error::Error + Send + Sync + 'static,
    {
        assert!(!evts.is_empty(), "evts must not be empty");

        // Persist all events transactionally.
        let mut cnn = self.cnn().await?;
        let tx = cnn.transaction().await.map_err(Error::StartTx)?;
        let stmt = tx
            .prepare("INSERT INTO evts VALUES ($1, $2, $3)")
            .await
            .map_err(Error::PrepareStmt)?;
        for (n, evt) in evts.iter().enumerate() {
            let seq_no = (last_seq_no + 1 + n as u64) as i64;
            let bytes =
                evt_to_bytes(evt).map_err(|source| Error::EvtsIntoBytes(Box::new(source)))?;
            tx.execute(&stmt, &[&id, &seq_no, &bytes.as_ref()])
                .await
                .map_err(Error::ExecuteStmt)?;
        }
        tx.commit().await.map_err(Error::CommitTx)?;

        Ok(None)
    }

    async fn last_seq_no(&self, id: Uuid) -> Result<u64, Self::Error> {
        let cnn = self.cnn().await?;
        let last_seq_no = cnn
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
        from_seq_no: u64,
        to_seq_no: u64,
        _metadata: Metadata,
        evt_from_bytes: EvtFromBytes,
    ) -> Result<impl Stream<Item = Result<(u64, E), Self::Error>> + 'a, Self::Error>
    where
        E: Send + 'a,
        EvtFromBytes: Fn(Bytes) -> Result<E, EvtFromBytesError> + Send + Sync + 'static,
        EvtFromBytesError: std::error::Error + Send + Sync + 'static,
    {
        assert!(from_seq_no > 0, "from_seq_no must be positive");
        assert!(
            from_seq_no <= to_seq_no,
            "from_seq_no must be less than or equal to to_seq_no"
        );

        debug!(%id, from_seq_no, to_seq_no, "Building event stream");

        let cnn = self.cnn().await?;
        let params: [&(dyn ToSql + Sync); 3] = [&id, &(from_seq_no as i64), &(to_seq_no as i64)];
        let evts = cnn
            .query_raw(
                "SELECT seq_no, evt FROM evts WHERE id = $1 AND seq_no >= $2 AND seq_no <= $3",
                params,
            )
            .await
            .map_err(Error::ExecuteStmt)?
            .map_err(Error::NextRow)
            .map_ok(move |row| {
                let seq_no = row.get::<_, i64>(0) as u64;
                let bytes = row.get::<_, &[u8]>(1);
                // TODO Can we do better?
                let bytes = Bytes::copy_from_slice(bytes);
                evt_from_bytes(bytes)
                    .map_err(|source| Error::EvtsFromBytes(Box::new(source)))
                    .map(|evt| (seq_no, evt))
            });

        let evts = stream! {
            for await evt in evts {
                let evt = evt.and_then(identity);
                let is_err = evt.is_err();
                yield evt;
                if is_err { break; }
            }
        };

        Ok(evts)
    }
}

type CnnPool<T> = Pool<PostgresConnectionManager<T>>;

type Cnn<'a, T> = PooledConnection<'a, PostgresConnectionManager<T>>;

/// Configuration for the [PostgresEvtLog].
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

/// Errors from the [PostgresEvtLog].
#[derive(Debug, Error)]
pub enum Error {
    /// Cannot create connection manager.
    #[error("Cannot create connection manager")]
    ConnectionManager(#[source] tokio_postgres::Error),

    /// Cannot create connection pool.
    #[error("Cannot create connection pool")]
    ConnectionPool(#[source] tokio_postgres::Error),

    /// Cannot get connection from pool.
    #[error("Cannot get connection from pool")]
    GetConnection(#[source] bb8_postgres::bb8::RunError<tokio_postgres::Error>),

    /// Cannot prepare statement.
    #[error("Cannot prepare statement")]
    PrepareStmt(#[source] tokio_postgres::Error),

    /// Cannot start transaction.
    #[error("Cannot start transaction")]
    StartTx(#[source] tokio_postgres::Error),

    /// Cannot commit transaction.
    #[error("Cannot commit transaction")]
    CommitTx(#[source] tokio_postgres::Error),

    /// Cannot execute statement.
    #[error("Cannot execute statement")]
    ExecuteStmt(#[source] tokio_postgres::Error),

    /// Cannot convert events to bytes.
    #[error("Cannot convert events to bytes")]
    EvtsIntoBytes(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),

    /// Cannot convert bytes to events.
    #[error("Cannot convert bytes to events")]
    EvtsFromBytes(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),

    /// Cannot get next row.
    #[error("Cannot get next row")]
    NextRow(#[source] tokio_postgres::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{array::TryFromSliceError, convert::Infallible, future};
    use testcontainers::{clients::Cli, images::postgres::Postgres};

    fn to_bytes(n: &i32) -> Result<Bytes, Infallible> {
        Ok(Bytes::copy_from_slice(&n.to_ne_bytes()))
    }

    fn from_bytes(bytes: Bytes) -> Result<i32, TryFromSliceError> {
        bytes.as_ref().try_into().map(i32::from_ne_bytes)
    }

    #[tokio::test]
    /// Directly testing the [PostgresEvtLog].
    async fn test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = Cli::default();
        let container = client.run(Postgres::default());
        let port = container.get_host_port_ipv4(5432);

        let config = Config::default().with_port(port);
        let mut evt_log = PostgresEvtLog::new(config).await?;
        evt_log.setup().await?;

        let id = Uuid::now_v7();

        let last_seq_no = evt_log.last_seq_no(id).await?;
        assert_eq!(last_seq_no, 0);

        evt_log
            .persist(id, [1, 2, 3].as_ref(), 0, &to_bytes)
            .await?;
        let last_seq_no = evt_log.last_seq_no(id).await?;
        assert_eq!(last_seq_no, 3);

        evt_log.persist(id, [4, 5].as_ref(), 3, &to_bytes).await?;
        let last_seq_no = evt_log.last_seq_no(id).await?;
        assert_eq!(last_seq_no, 5);

        let evts = evt_log
            .evts_by_id::<i32, _, _>(id, 2, 4, None, &from_bytes)
            .await?;
        let sum = evts
            .try_fold(0i32, |acc, (_, n)| future::ready(Ok(acc + n)))
            .await?;
        assert_eq!(sum, 9);

        Ok(())
    }
}
