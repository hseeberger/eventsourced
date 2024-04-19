use anyhow::{Context, Result};
use configured::Configured;
use eventsourced_postgres::{
    PostgresEventLog, PostgresEventLogConfig, PostgresSnapshotStore, PostgresSnapshotStoreConfig,
};
use serde::Deserialize;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .try_init()
        .context("initialize tracing")?;

    let config = Config::load().context("load configuration")?;
    println!("Starting with configuration: {config:?}");

    let event_log = PostgresEventLog::new(config.event_log)
        .await
        .context("create event log")?;

    let snapshot_store = PostgresSnapshotStore::new(config.snapshot_store)
        .await
        .context("create snapshot store")?;

    counter::run(config.counter, event_log, snapshot_store).await
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Config {
    counter: counter::Config,
    event_log: PostgresEventLogConfig,
    snapshot_store: PostgresSnapshotStoreConfig,
}
