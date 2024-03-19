use error_ext::StdErrorExt;
use eventsourced::{binarize, evt_log::EvtLog};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Postgres, Row, Transaction};
use std::{
    error::Error as StdError,
    fmt::Debug,
    num::{NonZeroU64, TryFromIntError},
    pin::pin,
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot, RwLock},
    task,
    time::sleep,
};
use tracing::{debug, error, info};

/// A projection of events of an event sourced entity to a Postgres database.
#[derive(Debug, Clone)]
pub struct Projection {
    name: String,
    cmd_in: mpsc::Sender<(Cmd, oneshot::Sender<State>)>,
}

impl Projection {
    pub async fn new<E, L, H>(
        type_name: &'static str,
        name: String,
        evt_log: L,
        evt_handler: H,
        error_strategy: ErrorStrategy,
        pool: Pool<Postgres>,
    ) -> Result<Self, Error>
    where
        E: for<'de> Deserialize<'de> + Send + 'static,
        L: EvtLog + Sync,
        H: EvtHandler<E> + Clone + Send + Sync + 'static,
    {
        sqlx::query(include_str!("create_projection.sql"))
            .execute(&pool)
            .await
            .expect("create projection table");

        let seq_no = load_seq_no(&name, &pool).await?;

        let state = Arc::new(RwLock::new(State {
            seq_no,
            running: false,
            error: None,
        }));

        let (cmd_in, mut cmd_out) = mpsc::channel::<(Cmd, oneshot::Sender<State>)>(1);

        task::spawn({
            let name = name.clone();
            let state = state.clone();

            async move {
                while let Some((cmd, reply_in)) = cmd_out.recv().await {
                    match cmd {
                        Cmd::Run => {
                            // Do not remove braces, dead-lock is waiting for you!
                            let running = { state.read().await.running };
                            if running {
                                info!(type_name, name, "projection already running");
                            } else {
                                info!(type_name, name, "running projection");

                                // Do not remove braces, dead-lock is waiting for you!
                                {
                                    let mut state = state.write().await;
                                    state.running = true;
                                    state.error = None;
                                }

                                run_projection_loop(
                                    type_name,
                                    name.clone(),
                                    state.clone(),
                                    evt_log.clone(),
                                    evt_handler.clone(),
                                    pool.clone(),
                                    error_strategy,
                                )
                                .await;
                            }

                            if reply_in.send(state.read().await.clone()).is_err() {
                                error!(type_name, name, "cannot send state");
                            }
                        }

                        Cmd::Stop => {
                            // Do not remove braces, dead-lock is waiting for you!
                            let running = { state.read().await.running };
                            if running {
                                info!(type_name, name, "stopping projection");
                                let mut state = state.write().await;
                                state.running = false;
                            } else {
                                info!(type_name, name, "projection already stopped");
                            }

                            if reply_in.send(state.read().await.clone()).is_err() {
                                error!(type_name, name, "cannot send state");
                            }
                        }

                        Cmd::GetState => {
                            if reply_in.send(state.read().await.clone()).is_err() {
                                error!(type_name, name, "cannot send state");
                            }
                        }
                    }
                }
            }
        });

        Ok(Projection { name, cmd_in })
    }

    pub async fn run(&self) -> Result<State, CmdError> {
        self.dispatch_cmd(Cmd::Run).await
    }

    pub async fn stop(&self) -> Result<State, CmdError> {
        self.dispatch_cmd(Cmd::Stop).await
    }

    pub async fn get_state(&self) -> Result<State, CmdError> {
        self.dispatch_cmd(Cmd::GetState).await
    }

    async fn dispatch_cmd(&self, cmd: Cmd) -> Result<State, CmdError> {
        let (reply_in, reply_out) = oneshot::channel();
        self.cmd_in
            .send((cmd, reply_in))
            .await
            .map_err(|_| CmdError::SendCmd(cmd, self.name.clone()))?;
        let state = reply_out
            .await
            .map_err(|_| CmdError::ReceiveResponse(cmd, self.name.clone()))?;
        Ok(state)
    }
}

#[trait_variant::make(Send)]
pub trait EvtHandler<E> {
    type Error: StdError + Send + Sync + 'static;

    async fn handle_evt(
        &self,
        evt: E,
        tx: &mut Transaction<'static, Postgres>,
    ) -> Result<(), Self::Error>;
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("cannot create Projection, b/c cannot load state from database")]
    Sqlx(#[from] sqlx::Error),

    #[error("cannot create Projection, b/c cannot convert loaded seq_no into non zero value")]
    TryFromInt(#[from] TryFromIntError),
}

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum CmdError {
    /// The command cannot be sent from this [Projection] to its projection.
    #[error("cannot send command {0:?} to projection {1}")]
    SendCmd(Cmd, String),

    /// A response for the command cannot be received from this [Projection]'s projection.
    #[error("cannot receive reply for command {0:?} from projection {1}")]
    ReceiveResponse(Cmd, String),
}

#[derive(Debug, Clone, Copy)]
pub enum ErrorStrategy {
    Retry(Duration),
    Stop,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct State {
    pub seq_no: Option<NonZeroU64>,
    pub running: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Cmd {
    Run,
    Stop,
    GetState,
}

#[derive(Debug, Error)]
enum IntenalRunError<E, H> {
    #[error(transparent)]
    Evts(E),

    #[error(transparent)]
    Handler(H),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    #[error(transparent)]
    LoadStateError(#[from] Error),
}

async fn load_seq_no(name: &str, pool: &Pool<Postgres>) -> Result<Option<NonZeroU64>, Error> {
    let seq_no = sqlx::query("SELECT seq_no FROM projection WHERE name=$1")
        .bind(name)
        .fetch_optional(pool)
        .await?
        .map(|row| row.try_get::<i64, _>(0))
        .transpose()?
        .map(|seq_no| (seq_no as u64).try_into())
        .transpose()?;
    Ok(seq_no)
}

async fn run_projection_loop<E, L, H>(
    type_name: &'static str,
    name: String,
    state: Arc<RwLock<State>>,
    evt_log: L,
    evt_handler: H,
    pool: Pool<Postgres>,
    error_strategy: ErrorStrategy,
) where
    E: for<'de> Deserialize<'de> + Send + 'static,
    L: EvtLog + Sync,
    H: EvtHandler<E> + Sync + 'static,
{
    task::spawn({
        async move {
            loop {
                match run_projection(type_name, &name, &evt_log, &evt_handler, &pool, &state).await
                {
                    Ok(_) => {
                        info!(type_name, name, "projection stopped");
                        {
                            let mut state = state.write().await;
                            state.running = false;
                        }
                        break;
                    }

                    Err(error) => {
                        error!(
                            error = error.as_chain(),
                            type_name, name, "projection error"
                        );

                        match error_strategy {
                            ErrorStrategy::Retry(delay) => {
                                info!(type_name, name, ?delay, "projection retrying after error");
                                {
                                    let mut state = state.write().await;
                                    state.error = Some(error.to_string());
                                }
                                sleep(delay).await
                            }

                            ErrorStrategy::Stop => {
                                info!(type_name, name, "projection stopped after error");
                                {
                                    let mut state = state.write().await;
                                    state.running = false;
                                    state.error = Some(error.to_string());
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }
    });
}

async fn run_projection<E, L, H>(
    type_name: &'static str,
    name: &str,
    evt_log: &L,
    handler: &H,
    pool: &Pool<Postgres>,
    state: &Arc<RwLock<State>>,
) -> Result<(), IntenalRunError<L::Error, H::Error>>
where
    E: for<'de> Deserialize<'de> + Send + 'static,
    L: EvtLog,
    H: EvtHandler<E>,
{
    let seq_no = load_seq_no(name, pool)
        .await?
        .map(|n| n.saturating_add(1))
        .unwrap_or(NonZeroU64::MIN);
    let evts = evt_log
        .evts_by_type::<E, _, _>(type_name, seq_no, binarize::serde_json::from_bytes)
        .await
        .map_err(IntenalRunError::Evts)?;
    let mut evts = pin!(evts);

    while let Some(evt) = evts.next().await {
        if !state.read().await.running {
            break;
        };

        let (seq_no, evt) = evt.map_err(IntenalRunError::Evts)?;

        let mut tx = pool.begin().await?;
        handler
            .handle_evt(evt, &mut tx)
            .await
            .map_err(IntenalRunError::Handler)?;
        debug!(type_name, name, seq_no, "projection handled event");
        save_seq_no(seq_no, name, &mut tx).await?;
        tx.commit().await?;

        state.write().await.seq_no = Some(seq_no);
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
    use crate::postgres::{ErrorStrategy, EvtHandler, Projection};
    use error_ext::BoxError;
    use eventsourced::{
        binarize::serde_json::to_bytes,
        evt_log::{test::TestEvtLog, EvtLog},
    };
    use sqlx::{
        postgres::{PgConnectOptions, PgPoolOptions},
        Postgres, QueryBuilder, Row, Transaction,
    };
    use std::{iter::once, time::Duration};
    use testcontainers::{clients::Cli, RunnableImage};
    use testcontainers_modules::postgres::Postgres as TCPostgres;
    use tokio::time::sleep;

    #[derive(Clone)]
    struct TestHandler;

    impl EvtHandler<i32> for TestHandler {
        type Error = sqlx::Error;

        async fn handle_evt(
            &self,
            evt: i32,
            tx: &mut Transaction<'static, Postgres>,
        ) -> Result<(), Self::Error> {
            QueryBuilder::new("INSERT INTO test (n) ")
                .push_values(once(evt), |mut q, evt| {
                    q.push_bind(evt);
                })
                .build()
                .execute(&mut **tx)
                .await?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test() -> Result<(), BoxError> {
        let containers = Cli::default();

        let container =
            containers.run(RunnableImage::from(TCPostgres::default()).with_tag("16-alpine"));
        let port = container.get_host_port_ipv4(5432);

        let cnn_url = format!("postgresql://postgres:postgres@localhost:{port}");
        let cnn_options = cnn_url.parse::<PgConnectOptions>()?;
        let pool = PgPoolOptions::new().connect_with(cnn_options).await?;

        let mut evt_log = TestEvtLog::<u64>::default();
        for n in 1..=100 {
            evt_log.persist("test", &0, None, &n, &to_bytes).await?;
        }

        sqlx::query("CREATE TABLE test (n bigint);")
            .execute(&pool)
            .await?;

        let projection = Projection::new(
            "test",
            "test-projection".to_string(),
            evt_log.clone(),
            TestHandler,
            ErrorStrategy::Stop,
            pool.clone(),
        )
        .await?;

        QueryBuilder::new("INSERT INTO projection ")
            .push_values(once(("test-projection", 10)), |mut q, (name, seq_no)| {
                q.push_bind(name).push_bind(seq_no);
            })
            .build()
            .execute(&pool)
            .await?;

        projection.run().await?;

        let mut state = projection.get_state().await?;
        let max = Some(100.try_into()?);
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
        assert_eq!(sum, 4_995); // sum(1..100) - sum(1..10)

        projection.stop().await?;
        sleep(Duration::from_millis(100)).await;
        let state = projection.get_state().await?;
        sleep(Duration::from_millis(100)).await;
        let state_2 = projection.get_state().await?;
        assert_eq!(state.seq_no, state_2.seq_no);

        Ok(())
    }
}
