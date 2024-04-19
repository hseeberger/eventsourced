//! [EventLog](eventsourced::event_log::EventLog) and
//! [SnapshotStore](eventsourced::snapshot_store::SnapshotStore) implementations based upon [NATS](https://nats.io/).

mod event_log;
mod snapshot_store;

pub use event_log::{Config as NatsEventLogConfig, NatsEventLog};
pub use snapshot_store::{Config as NatsSnapshotStoreConfig, NatsSnapshotStore};

use async_nats::{Client, ConnectOptions};
use error_ext::BoxError;
use prost::{DecodeError, EncodeError};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use std::path::PathBuf;
use thiserror::Error;

/// Authentication configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum AuthConfig {
    UserPassword {
        user: String,
        password: SecretString,
    },
    CredentialsFile(PathBuf),
}

/// Errors from the [NatsEventLog] or [NatsSnapshotStore].
#[derive(Debug, Error)]
pub enum Error {
    #[error("NATS error: {0}")]
    Nats(String, #[source] Box<dyn std::error::Error + Send + Sync>),

    /// Event cannot be converted into bytes.
    #[error("cannot convert event to bytes")]
    ToBytes(#[source] BoxError),

    /// Bytes cannot be converted to event.
    #[error("cannot convert bytes to event")]
    FromBytes(#[source] BoxError),

    /// Snapshot cannot be encoded as Protocol Buffers.
    #[error("cannot encode snapshot as Protocol Buffers")]
    EncodeSnapshot(#[from] EncodeError),

    /// Snapshot cannot be decoded from Protocol Buffers.
    #[error("cannot decode snapshot from Protocol Buffers")]
    DecodeSnapshot(#[from] DecodeError),

    /// Invalid sequence number.
    #[error("invalid sequence number")]
    InvalidNonZeroU64,
}

/// Create a NATS client.
pub async fn make_client(auth: Option<&AuthConfig>, server_addr: &str) -> Result<Client, Error> {
    let mut options = ConnectOptions::new();
    if let Some(auth) = auth {
        match auth {
            AuthConfig::UserPassword { user, password } => {
                options =
                    options.user_and_password(user.to_owned(), password.expose_secret().to_owned());
            }

            AuthConfig::CredentialsFile(credentials) => {
                options = options
                    .credentials_file(credentials)
                    .await
                    .map_err(|error| {
                        Error::Nats(
                            format!(
                                "cannot read NATS credentials file at {})",
                                credentials.display()
                            ),
                            error.into(),
                        )
                    })?;
            }
        }
    }
    let client = options.connect(server_addr).await.map_err(|error| {
        Error::Nats(
            format!("cannot connect to NATS server at {server_addr})"),
            error.into(),
        )
    })?;
    Ok(client)
}

#[cfg(test)]
pub mod tests {
    use crate::AuthConfig;
    use assert_matches::assert_matches;
    use config::{Config, File, FileFormat};
    use secrecy::ExposeSecret;

    pub const NATS_VERSION: &str = "2.10-alpine";

    #[test]
    fn test_deserialize_auth_config() {
        let auth = "user: test\npassword: test";
        let config = Config::builder()
            .add_source(File::from_str(&auth, FileFormat::Yaml))
            .build()
            .unwrap();
        let result = config.try_deserialize::<AuthConfig>();
        assert_matches!(
            result,
            Ok(AuthConfig::UserPassword { user, password })
                if user =="test" && password.expose_secret() == "test"
        );
    }
}
