use eventsourced::{convert, error_chain, EventSourced, EvtLog};
use futures::StreamExt;
use serde::Deserialize;
use sqlx::{Pool, Postgres, Transaction};
use std::{
    error::Error as StdError, fmt::Debug, num::NonZeroU64, pin::pin, sync::Arc, time::Duration,
};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot, RwLock},
    task,
    time::sleep,
};
use tracing::{debug, error, info};

#[derive(Debug, Clone)]
pub struct Projection {
    name: String,
    cmd_in: mpsc::Sender<(Cmd, oneshot::Sender<State>)>,
}

impl Projection {
    pub async fn spawn<E, L, H>(
        name: String,
        evt_log: L,
        evt_handler: H,
        error_strategy: ErrorStrategy,
        pool: Pool<Postgres>,
    ) -> Projection
    where
        E: EventSourced,
        E::Evt: for<'de> Deserialize<'de> + 'static,
        L: EvtLog + Send + Sync,
        H: EvtHandler<EventSourced = E> + Send + Sync + Clone + 'static,
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
                                    let evt_log = evt_log.clone();
                                    let evt_handler = evt_handler.clone();
                                    let pool = pool.clone();

                                    async move {
                                        loop {
                                            let run_result = run(
                                                &name,
                                                &evt_log,
                                                &evt_handler,
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
    type EventSourced: EventSourced;

    type Error: StdError + Send + Sync + 'static;

    async fn handle_evt(
        &self,
        evt: <Self::EventSourced as EventSourced>::Evt,
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

async fn run<E, L, H>(
    name: &str,
    evt_log: &L,
    handler: &H,
    pool: &Pool<Postgres>,
    state: Arc<RwLock<State>>,
) -> Result<(), RunError<L::Error, H::Error>>
where
    E: EventSourced,
    E::Evt: for<'de> Deserialize<'de> + 'static,
    L: EvtLog,
    H: EvtHandler<EventSourced = E>,
{
    let evts = evt_log
        .evts_by_type::<E, _, _>(state.read().await.seq_no, convert::serde_json::from_bytes)
        .await
        .map_err(RunError::Evts)?;
    let mut evts = pin!(evts);

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
    use bytes::Bytes;
    use futures::{stream, Stream};
    use sqlx::{
        postgres::{PgConnectOptions, PgPoolOptions},
        Row,
    };
    use std::convert::Infallible;
    use testcontainers::{clients::Cli, RunnableImage};
    use testcontainers_modules::postgres::Postgres as TCPostgres;
    use uuid::Uuid;

    #[derive(Debug)]
    struct Dummy;

    impl EventSourced for Dummy {
        type Id = Uuid;
        type Cmd = ();
        type Evt = i32;
        type State = u64;
        type Error = Infallible;

        const TYPE_NAME: &'static str = "simple";

        fn handle_cmd(
            _id: &Self::Id,
            _state: &Self::State,
            _cmd: Self::Cmd,
        ) -> Result<Self::Evt, Self::Error> {
            todo!()
        }

        fn handle_evt(_state: Self::State, _evt: Self::Evt) -> Self::State {
            todo!()
        }
    }

    #[derive(Debug, Clone)]
    struct TestEvtLog;

    impl EvtLog for TestEvtLog {
        type Id = Uuid;
        type Error = TestEvtLogError;

        async fn persist<E, ToBytes, ToBytesError>(
            &mut self,
            _evt: &E::Evt,
            _id: &Self::Id,
            last_seq_no: Option<NonZeroU64>,
            _to_bytes: &ToBytes,
        ) -> Result<NonZeroU64, Self::Error>
        where
            E: EventSourced,
            ToBytes: Fn(&E::Evt) -> Result<Bytes, ToBytesError> + Sync,
            ToBytesError: StdError + Send + Sync + 'static,
        {
            let seq_no = last_seq_no.unwrap_or(NonZeroU64::MIN);
            Ok(seq_no)
        }

        async fn last_seq_no<E>(
            &self,
            _entity_id: &Self::Id,
        ) -> Result<Option<NonZeroU64>, Self::Error>
        where
            E: EventSourced,
        {
            Ok(Some(42.try_into().unwrap()))
        }

        async fn evts_by_id<E, FromBytes, FromBytesError>(
            &self,
            _id: &Self::Id,
            _seq_no: NonZeroU64,
            _evt_from_bytes: FromBytes,
        ) -> Result<impl Stream<Item = Result<(NonZeroU64, E::Evt), Self::Error>> + Send, Self::Error>
        where
            E: EventSourced,
            FromBytes: Fn(Bytes) -> Result<E::Evt, FromBytesError> + Copy + Send + Sync,
            FromBytesError: StdError + Send + Sync + 'static,
        {
            Ok(stream::empty())
        }

        async fn evts_by_type<E, FromBytes, FromBytesError>(
            &self,
            seq_no: NonZeroU64,
            evt_from_bytes: FromBytes,
        ) -> Result<impl Stream<Item = Result<(NonZeroU64, E::Evt), Self::Error>> + Send, Self::Error>
        where
            E: EventSourced,
            FromBytes: Fn(Bytes) -> Result<E::Evt, FromBytesError> + Copy + Send + Sync,
            FromBytesError: StdError + Send + Sync + 'static,
        {
            let evts = stream::iter(seq_no.get()..=100).map(move |n| {
                let evt = n as i64;
                let n = NonZeroU64::new(n).unwrap();
                let evt = evt_from_bytes(serde_json::to_vec(&evt).unwrap().into()).unwrap();
                Ok((n, evt))
            });

            Ok(evts)
        }
    }

    #[derive(Debug, Error)]
    #[error("TestEvtLogError")]
    struct TestEvtLogError(#[source] Box<dyn StdError + Send + Sync>);

    #[derive(Clone)]
    struct TestHandler;

    impl EvtHandler for TestHandler {
        type EventSourced = Dummy;

        type Error = sqlx::Error;

        async fn handle_evt(
            &self,
            evt: <Self::EventSourced as EventSourced>::Evt,
            tx: &mut Transaction<'static, Postgres>,
        ) -> Result<(), Self::Error> {
            let query = "INSERT INTO test (n) VALUES ($1)";
            sqlx::query(query).bind(evt).execute(&mut **tx).await?;
            Ok(())
        }
    }

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

        let projection = Projection::spawn(
            "test-projection".to_string(),
            TestEvtLog,
            TestHandler,
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
