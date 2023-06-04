use anyhow::{Context, Result};
use configured::Configured;
use eventsourced_postgres::{
    PostgresEvtLog, PostgresEvtLogConfig, PostgresSnapshotStore, PostgresSnapshotStoreConfig,
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

    let evt_log = PostgresEvtLog::new(config.evt_log)
        .await
        .context("create event log")?;

    let snapshot_store = PostgresSnapshotStore::new(config.snapshot_store)
        .await
        .context("create snapshot store")?;

    counter::run(config.counter, evt_log, snapshot_store).await
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Config {
    counter: counter::Config,
    evt_log: PostgresEvtLogConfig,
    snapshot_store: PostgresSnapshotStoreConfig,
}
