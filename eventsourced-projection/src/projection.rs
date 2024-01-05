use anyhow::anyhow;
use futures::{Stream, StreamExt};
use sqlx::{database::HasArguments, query::Query, Pool, Postgres, Transaction};
use std::{error::Error as StdError, fmt::Debug, num::NonZeroU64, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot, RwLock},
    task,
    time::sleep,
};
use tower::{Service, ServiceExt};
use tracing::{debug, error, info};

type HandlerResult<'q, E> =
    Result<Query<'q, Postgres, <Postgres as HasArguments<'q>>::Arguments>, E>;

#[derive(Debug, Clone)]
pub struct Projection {
    name: String,
    cmd_in: mpsc::Sender<(Cmd, oneshot::Sender<State>)>,
}

impl Projection {
    pub async fn run(&self) -> Result<(), Error> {
        let (cmd_in, _) = oneshot::channel();
        self.cmd_in
            .send((Cmd::Run, cmd_in))
            .await
            .map_err(|_| Error::SendCmd("Run", self.name.clone()))?;
        Ok(())
    }

    pub async fn stop(&self) -> Result<(), Error> {
        let (cmd_in, _) = oneshot::channel();
        self.cmd_in
            .send((Cmd::Stop, cmd_in))
            .await
            .map_err(|_| Error::SendCmd("Stop", self.name.clone()))?;
        Ok(())
    }

    pub async fn get_state(&self) -> Result<State, Error> {
        let (cmd_in, state_out) = oneshot::channel();
        self.cmd_in
            .send((Cmd::GetState, cmd_in))
            .await
            .map_err(|_| Error::SendCmd("GetState", self.name.clone()))?;
        let state = state_out
            .await
            .map_err(|_| Error::ReceiveReply("GetState", self.name.clone()))?;
        Ok(state)
    }
}

#[derive(Debug, Error)]
pub enum Error {
    /// A command cannot be sent from this [Projection] to its projection.
    #[error("cannot send {0} command to projection {1}")]
    SendCmd(&'static str, String),

    /// A reply for a command cannot be received from this [Projection]'s projection.
    #[error("cannot receive reply for {0} command from projection {1}")]
    ReceiveReply(&'static str, String),
}

#[derive(Debug)]
pub enum ErrorStrategy {
    Retry(Duration),
    Stop,
}

#[derive(Debug, Clone)]
pub struct State {
    seq_no: NonZeroU64,
    running: bool,
    error: Option<String>,
}

pub async fn spawn<'q, E, M, H, EvtsError, HandlerError>(
    name: String,
    mut make_evts: M,
    handle_evt: H,
    error_strategy: ErrorStrategy,
    pool: Pool<Postgres>,
) -> Projection
where
    E: Send,
    M: Service<NonZeroU64, Error = EvtsError> + Send + 'static,
    M::Response: Stream<Item = Result<(NonZeroU64, E), EvtsError>> + Send + Unpin,
    M::Future: Send,
    EvtsError: std::error::Error + Send + Sync + 'static,
    H: Fn(E) -> HandlerResult<'q, HandlerError> + Send + Sync + 'static,
    HandlerError: std::error::Error + Send + Sync + 'static,
{
    let state = Arc::new(RwLock::new(State {
        seq_no: NonZeroU64::MIN,
        running: false,
        error: None,
    }));

    let (cmd_in, mut cmd_out) = mpsc::channel::<(Cmd, oneshot::Sender<State>)>(1);

    task::spawn({
        let name = name.clone();
        let state = state.clone();

        async move {
            while let Some((cmd, result_sender)) = cmd_out.recv().await {
                match cmd {
                    Cmd::Run => {
                        let mut state = state.write().await;
                        if state.running {
                            info!(name, "projection already running");
                        } else {
                            info!(name, "running projection");
                            state.running = true;
                            state.error = None;
                        }
                    }

                    Cmd::Stop => {
                        let running = &mut state.write().await.running;
                        if !*running {
                            info!(name, "projection already stopped");
                        } else {
                            info!(name, "stopping projection");
                            *running = false;
                            todo!()
                        }
                    }

                    Cmd::GetState => {
                        let state = state.read().await.clone();
                        if result_sender.send(state).is_err() {
                            error!(name, "cannot send state");
                        }
                    }
                }
            }
        }
    });

    task::spawn({
        let name = name.clone();
        async move {
            loop {
                let seq_no = state.read().await.seq_no;
                debug!(seq_no, "executing projection step");

                let run_result =
                    run(&name, &mut make_evts, &handle_evt, &pool, state.clone()).await;
                match run_result {
                    Ok(_) => error!("projection terminated unexpectedly"),

                    Err(error) => {
                        error!(
                            error = format!("{}", anyhow!(error)),
                            "projection step terminated with error"
                        );

                        match error_strategy {
                            ErrorStrategy::Retry(delay) => {
                                info!(?delay, "retrying");
                                sleep(delay).await
                            }

                            ErrorStrategy::Stop => {
                                info!("stopping");
                                break;
                            }
                        }
                    }
                }
            }
        }
    });

    Projection { name, cmd_in }
}

#[derive(Debug)]
enum Cmd {
    Run,
    Stop,
    GetState,
}

async fn run<'q, E, M, H, EvtsError, HandlerError>(
    name: &str,
    make_evts: &mut M,
    handle_evt: &H,
    pool: &Pool<Postgres>,
    state: Arc<RwLock<State>>,
) -> Result<(), Box<dyn StdError + Send + Sync + 'static>>
where
    M: Service<NonZeroU64, Error = EvtsError>,
    M::Response: Stream<Item = Result<(NonZeroU64, E), EvtsError>> + Send + Unpin,
    EvtsError: std::error::Error + Send + Sync + 'static,
    H: Fn(E) -> HandlerResult<'q, HandlerError>,
    HandlerError: std::error::Error + Send + Sync + 'static,
{
    let make_evts = make_evts.ready().await?;
    let mut evts = make_evts.call(state.read().await.seq_no).await?;

    while let Some(evt) = evts.next().await {
        if !state.read().await.running {
            break;
        };

        let (seq_no, evt) = evt?;

        let mut tx = pool.begin().await?;
        handle_evt(evt)?.execute(&mut *tx).await?;
        save_seq_no(seq_no, name, &mut tx).await?;
        tx.commit().await?;

        state.write().await.seq_no = seq_no;
    }

    Ok(())
}

async fn save_seq_no(
    seq_no: NonZeroU64,
    name: &str,
    tx: &mut Transaction<'_, Postgres>,
) -> Result<(), sqlx::Error> {
    let query = r#"INSERT INTO projection (name, seq_no)
                   VAULES ($1, $2)
                   ON CONFLICT (name) DO UPDATE SET seq_no = $2"#;
    sqlx::query(query)
        .bind(name)
        .bind(seq_no.get() as i64)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
