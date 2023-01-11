use anyhow::{anyhow, Context, Result};
use configured::Configured;
use eventsourced_nats::{NatsEvtLog, NatsEvtLogConfig, NatsSnapshotStore, NatsSnapshotStoreConfig};
use serde::Deserialize;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .try_init()
        .context("Cannot initialize tracing")?;

    let config = Config::load().context("Cannot load configuration")?;
    println!("Starting with configuration: {config:?}");

    let evt_log = NatsEvtLog::new(NatsEvtLogConfig::default())
        .await
        .context("Cannot create event log")?;
    evt_log.setup().await.context("Cannot setup event log")?;

    let snapshot_store = NatsSnapshotStore::new(NatsSnapshotStoreConfig::default())
        .await
        .context("Cannot create snapshot store")?;
    snapshot_store
        .setup()
        .await
        .map_err(|error| anyhow!(error))
        .context("Cannot setup snapshot store")?;

    counter::run(config.counter, evt_log, snapshot_store).await
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Config {
    counter: counter::Config,
}
