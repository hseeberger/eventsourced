use anyhow::{Context, Result};
use eventsourced::evt_log::{
    nats::{Config, NatsEvtLog},
    EvtLog,
};
use futures::StreamExt;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

/// Not a real example, but rather a integration test, because the event log is not supposed to be
/// used directly.
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::filter::LevelFilter::INFO)
        .with(tracing_subscriber::fmt::layer())
        .try_init()
        .context("Cannot initialize tracing")?;

    // Create EvtLog
    let mut evt_log = NatsEvtLog::new(Config::default())
        .await
        .context("Cannot create NatsEvtLog")?;

    // Create entity ID
    let id = Uuid::now_v7();

    // Persist events
    evt_log
        .persist(id, &[1, 2, 3, 4], 1000)
        .await
        .context("Cannot persist events")?;
    evt_log
        .persist(id, &[5, 6, 7], 1004)
        .await
        .context("Cannot persist events")?;
    evt_log
        .persist(id, &[8, 9], 1007)
        .await
        .context("Cannot persist events")?;
    info!("Persisted events");

    // Get events
    let evts = evt_log
        .evts_by_id::<u32>(id, 1002, 1008)
        .await
        .context("Cannot get events")?;
    info!("Got events");
    let evts = evts.collect::<Vec<_>>().await;
    info!("evts: {evts:?}");

    Ok(())
}
