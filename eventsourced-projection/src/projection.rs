use eventsourced::error_chain;
use futures::{Stream, StreamExt};
use sqlx::{Pool, Postgres, Transaction};
use std::{error::Error as StdError, fmt::Debug, num::NonZeroU64, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot, RwLock},
    task,
    time::sleep,
};
use tower::{Service, ServiceExt};
use tracing::{debug, error, info};

#[derive(Debug, Clone)]
pub struct Projection {
    name: String,
    cmd_in: mpsc::Sender<(Cmd, oneshot::Sender<State>)>,
}

impl Projection {
    pub async fn spawn<M, H, EvtsError>(
        name: String,
        make_evts: M,
        handle_evt: H,
        error_strategy: ErrorStrategy,
        pool: Pool<Postgres>,
    ) -> Projection
    where
        M: Service<NonZeroU64, Error = EvtsError> + Send + 'static + Clone,
        M::Response: Stream<Item = Result<(NonZeroU64, H::Evt), EvtsError>> + Send + Unpin,
        M::Future: Send,
        EvtsError: StdError + Send + Sync + 'static,
        H: EvtHandler + Send + Sync + Clone + 'static,
    {
        sqlx::query(include_str!("create_projection.sql"))
            .execute(&pool)
            .await
            .expect("create projection table");

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
                            let running = { state.read().await.running };
                            if running {
                                info!(name, "projection already running");
                            } else {
                                info!(name, "running projection");
                                {
                                    let mut state = state.write().await;
                                    state.running = true;
                                    state.error = None;
                                }

                                task::spawn({
                                    let name = name.clone();
                                    let state = state.clone();
                                    let mut make_evts = make_evts.clone();
                                    let handle_evt = handle_evt.clone();
                                    let pool = pool.clone();

                                    async move {
                                        loop {
                                            let run_result = run(
                                                &name,
                                                &mut make_evts,
                                                &handle_evt,
                                                &pool,
                                                state.clone(),
                                            )
                                            .await;

                                            match run_result {
                                                Ok(_) => {
                                                    info!(name, "stopped");
                                                    break;
                                                }

                                                Err(error) => {
                                                    error!(
                                                        name,
                                                        error = error_chain(error),
                                                        "projection terminated with error"
                                                    );

                                                    match error_strategy {
                                                        ErrorStrategy::Retry(delay) => {
                                                            info!(
                                                                name,
                                                                ?delay,
                                                                "retrying after error"
                                                            );
                                                            sleep(delay).await
                                                        }

                                                        ErrorStrategy::Stop => {
                                                            info!(name, "stopping after error");
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                });
                            }
                        }

                        Cmd::Stop => {
                            let running = &mut state.write().await.running;
                            if !*running {
                                info!(name, "projection already stopped");
                            } else {
                                info!(name, "stopping projection");
                                *running = false;
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

        Projection { name, cmd_in }
    }

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

#[trait_variant::make(EvtHandler: Send)]
pub trait LocalEvtHandler {
    type Evt: Send;

    type Error: StdError + Send + Sync + 'static;

    async fn handle_evt(
        &self,
        evt: Self::Evt,
        tx: &mut Transaction<'static, Postgres>,
    ) -> Result<(), Self::Error>;
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

#[derive(Debug, Clone, Copy)]
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

#[derive(Debug)]
enum Cmd {
    Run,
    Stop,
    GetState,
}

#[derive(Debug, Error)]
enum RunError<E, H> {
    #[error(transparent)]
    Evts(E),

    #[error(transparent)]
    Handler(H),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

async fn run<M, H, EvtsError>(
    name: &str,
    make_evts: &mut M,
    handler: &H,
    pool: &Pool<Postgres>,
    state: Arc<RwLock<State>>,
) -> Result<(), RunError<EvtsError, H::Error>>
where
    M: Service<NonZeroU64, Error = EvtsError>,
    M::Response: Stream<Item = Result<(NonZeroU64, H::Evt), EvtsError>> + Send + Unpin,
    EvtsError: StdError + Send + Sync + 'static,
    H: EvtHandler,
{
    let make_evts = make_evts.ready().await.map_err(RunError::Evts)?;
    let mut evts = make_evts
        .call(state.read().await.seq_no)
        .await
        .map_err(RunError::Evts)?;

    while let Some(evt) = evts.next().await {
        if !state.read().await.running {
            break;
        };

        let (seq_no, evt) = evt.map_err(RunError::Evts)?;

        let mut tx = pool.begin().await?;
        handler
            .handle_evt(evt, &mut tx)
            .await
            .map_err(RunError::Handler)?;
        debug!(seq_no, "handled event");
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
                   VALUES ($1, $2)
                   ON CONFLICT (name) DO UPDATE SET seq_no = $2"#;
    sqlx::query(query)
        .bind(name)
        .bind(seq_no.get() as i64)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use futures::{future::ok, stream};
    use sqlx::{
        postgres::{PgConnectOptions, PgPoolOptions},
        Row,
    };
    use std::convert::Infallible;
    use testcontainers::{clients::Cli, RunnableImage};
    use testcontainers_modules::postgres::Postgres as TCPostgres;
    use tower::service_fn;

    #[tokio::test]
    async fn test() -> Result<(), Box<dyn StdError>> {
        let testcontainers_client = Cli::default();
        let container = testcontainers_client
            .run(RunnableImage::from(TCPostgres::default()).with_tag("16-alpine"));
        let port = container.get_host_port_ipv4(5432);

        let cnn_url = format!("postgresql://postgres:postgres@localhost:{port}");
        let cnn_options = cnn_url
            .parse::<PgConnectOptions>()
            .context("parse PgConnectOptions")?;
        let pool = PgPoolOptions::new().connect_with(cnn_options).await?;

        sqlx::query("CREATE TABLE test (n bigint PRIMARY KEY);")
            .execute(&pool)
            .await?;

        let make_evts = service_fn(|seq_no: NonZeroU64| {
            ok::<_, Infallible>(
                stream::iter(seq_no.get()..=100)
                    .map(|n| Ok::<_, Infallible>((NonZeroU64::new(n).unwrap(), n as i64))),
            )
        });

        #[derive(Clone)]
        struct H;
        impl EvtHandler for H {
            type Evt = i64;

            type Error = sqlx::Error;

            async fn handle_evt(
                &self,
                evt: Self::Evt,
                tx: &mut Transaction<'static, Postgres>,
            ) -> Result<(), Self::Error> {
                let query = "INSERT INTO test (n) VALUES ($1)";
                sqlx::query(query).bind(evt).execute(&mut **tx).await?;
                Ok(())
            }
        }

        let projection = Projection::spawn(
            "test-projection".to_string(),
            make_evts,
            H,
            ErrorStrategy::Stop,
            pool.clone(),
        )
        .await;

        projection.run().await?;

        let mut state = projection.get_state().await?;
        let max = NonZeroU64::new(100).unwrap();
        while state.seq_no < max {
            sleep(Duration::from_millis(100)).await;
            state = projection.get_state().await?;
        }
        assert_eq!(state.seq_no, max);

        let sum = sqlx::query("SELECT * FROM test;")
            .fetch_all(&pool)
            .await?
            .into_iter()
            .map(|row| row.try_get::<i64, _>(0))
            .try_fold(0i64, |acc, n| n.map(|n| acc + n))?;
        assert_eq!(sum, 5050);

        projection.stop().await?;
        sleep(Duration::from_millis(100)).await;
        let state = projection.get_state().await?;
        sleep(Duration::from_millis(100)).await;
        let state_2 = projection.get_state().await?;
        assert_eq!(state.seq_no, state_2.seq_no);

        Ok(())
    }
}
