use error_ext::StdErrorExt;
use eventsourced::{binarize, event_log::EventLog};
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
    command_in: mpsc::Sender<(Command, oneshot::Sender<State>)>,
}

impl Projection {
    pub async fn new<E, L, H>(
        type_name: &'static str,
        name: String,
        event_log: L,
        event_handler: H,
        error_strategy: ErrorStrategy,
        pool: Pool<Postgres>,
    ) -> Result<Self, Error>
    where
        E: for<'de> Deserialize<'de> + Send + 'static,
        L: EventLog + Sync,
        H: EventHandler<E> + Clone + Send + Sync + 'static,
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

        let (command_in, mut command_out) = mpsc::channel::<(Command, oneshot::Sender<State>)>(1);

        task::spawn({
            let name = name.clone();
            let state = state.clone();

            async move {
                while let Some((command, reply_in)) = command_out.recv().await {
                    match command {
                        Command::Run => {
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
                                    event_log.clone(),
                                    event_handler.clone(),
                                    pool.clone(),
                                    error_strategy,
                                )
                                .await;
                            }

                            if reply_in.send(state.read().await.clone()).is_err() {
                                error!(type_name, name, "cannot send state");
                            }
                        }

                        Command::Stop => {
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

                        Command::GetState => {
                            if reply_in.send(state.read().await.clone()).is_err() {
                                error!(type_name, name, "cannot send state");
                            }
                        }
                    }
                }
            }
        });

        Ok(Projection { name, command_in })
    }

    pub async fn run(&self) -> Result<State, CommandError> {
        self.dispatch_command(Command::Run).await
    }

    pub async fn stop(&self) -> Result<State, CommandError> {
        self.dispatch_command(Command::Stop).await
    }

    pub async fn get_state(&self) -> Result<State, CommandError> {
        self.dispatch_command(Command::GetState).await
    }

    async fn dispatch_command(&self, command: Command) -> Result<State, CommandError> {
        let (reply_in, reply_out) = oneshot::channel();
        self.command_in
            .send((command, reply_in))
            .await
            .map_err(|_| CommandError::SendCommand(command, self.name.clone()))?;
        let state = reply_out
            .await
            .map_err(|_| CommandError::ReceiveResponse(command, self.name.clone()))?;
        Ok(state)
    }
}

#[trait_variant::make(Send)]
pub trait EventHandler<E> {
    type Error: StdError + Send + Sync + 'static;

    async fn handle_event(
        &self,
        event: E,
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
pub enum CommandError {
    /// The command cannot be sent from this [Projection] to its projection.
    #[error("cannot send command {0:?} to projection {1}")]
    SendCommand(Command, String),

    /// A response for the command cannot be received from this [Projection]'s projection.
    #[error("cannot receive reply for command {0:?} from projection {1}")]
    ReceiveResponse(Command, String),
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
pub enum Command {
    Run,
    Stop,
    GetState,
}

#[derive(Debug, Error)]
enum IntenalRunError<E, H> {
    #[error(transparent)]
    Events(E),

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
    event_log: L,
    event_handler: H,
    pool: Pool<Postgres>,
    error_strategy: ErrorStrategy,
) where
    E: for<'de> Deserialize<'de> + Send + 'static,
    L: EventLog + Sync,
    H: EventHandler<E> + Sync + 'static,
{
    task::spawn({
        async move {
            loop {
                let result =
                    run_projection(type_name, &name, &event_log, &event_handler, &pool, &state)
                        .await;
                match result {
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
    event_log: &L,
    handler: &H,
    pool: &Pool<Postgres>,
    state: &Arc<RwLock<State>>,
) -> Result<(), IntenalRunError<L::Error, H::Error>>
where
    E: for<'de> Deserialize<'de> + Send + 'static,
    L: EventLog,
    H: EventHandler<E>,
{
    let seq_no = load_seq_no(name, pool)
        .await?
        .map(|n| n.saturating_add(1))
        .unwrap_or(NonZeroU64::MIN);
    let events = event_log
        .events_by_type::<E, _, _>(type_name, seq_no, binarize::serde_json::from_bytes)
        .await
        .map_err(IntenalRunError::Events)?;
    let mut events = pin!(events);

    while let Some(event) = events.next().await {
        if !state.read().await.running {
            break;
        };

        let (seq_no, event) = event.map_err(IntenalRunError::Events)?;

        let mut tx = pool.begin().await?;
        handler
            .handle_event(event, &mut tx)
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
    use crate::postgres::{ErrorStrategy, EventHandler, Projection};
    use error_ext::BoxError;
    use eventsourced::{
        binarize::serde_json::to_bytes,
        event_log::{test::TestEventLog, EventLog},
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

    impl EventHandler<i32> for TestHandler {
        type Error = sqlx::Error;

        async fn handle_event(
            &self,
            event: i32,
            tx: &mut Transaction<'static, Postgres>,
        ) -> Result<(), Self::Error> {
            QueryBuilder::new("INSERT INTO test (n) ")
                .push_values(once(event), |mut q, event| {
                    q.push_bind(event);
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

        let mut event_log = TestEventLog::<u64>::default();
        for n in 1..=100 {
            event_log.persist("test", &0, None, &[n], &to_bytes).await?;
        }

        sqlx::query("CREATE TABLE test (n bigint);")
            .execute(&pool)
            .await?;

        let projection = Projection::new(
            "test",
            "test-projection".to_string(),
            event_log.clone(),
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
