#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]
#![feature(return_position_impl_trait_in_trait)]
#![allow(clippy::type_complexity)]

mod event_log;

pub use event_log::*;

use futures::StreamExt;
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt::Debug, marker::PhantomData};
use thiserror::Error;
use tokio::{
    pin,
    sync::{mpsc, oneshot},
    task,
};
use tracing::{debug, error, info};
use uuid::Uuid;

/// EventSourced errors.
#[derive(Debug, Error)]
pub enum Error<E, L>
where
    E: EventSourced + 'static,
    L: EvtLog + 'static,
{
    /// A command cannot be sent from an [EntityRef] to its [Entity].
    #[error("Cannot send command to entity with ID {id}")]
    Send {
        id: Uuid,
        source: mpsc::error::SendError<(E::Cmd, oneshot::Sender<Result<Vec<E::Evt>, E::Error>>)>,
    },

    /// Events or an [EventLog] error cannot be sent from an [Entity] to the invoking [EntityRef].
    #[error("Cannot receive command handling result from entity with ID {id}")]
    Rcv {
        id: Uuid,
        source: oneshot::error::RecvError,
    },

    /// A command handler of an [Entity] has rejected a command.
    #[error("Command handler error for entity with ID {id}")]
    CmdHandler { id: Uuid, source: E::Error },

    #[error("Cannot restore entity with ID {id}")]
    Restore { id: Uuid, source: L::Error },
}

/// Command and event handling for an [Entity].
pub trait EventSourced {
    /// Command type.
    type Cmd: Debug;

    /// Event type.
    type Evt: Debug;

    /// Error type for the command handler.
    type Error: std::error::Error;

    /// Command handler.
    fn handle_cmd(&self, cmd: Self::Cmd) -> Result<Vec<Self::Evt>, Self::Error>;

    /// Event handler.
    fn handle_evt(&mut self, evt: &Self::Evt);
}

/// An entity, uniquely identifiable by its ID, able to handle commands via its [EventSourced]
/// value. If a command successfully produces events, these get persisted to its [EvtLog].
pub struct Entity<E, L> {
    id: Uuid,
    event_sourced: E,
    evt_log: L,
    seq_no: u64,
}

impl<E, L> Entity<E, L>
where
    E: EventSourced + Send + 'static,
    E::Cmd: Send + 'static,
    E::Evt: Serialize + DeserializeOwned + Send + Sync,
    E::Error: Send,
    L: EvtLog + Send + 'static,
{
    /// Create an [Entity] with the given ID, [EventSourced] command/event handling and [EventLog].
    /// Commands can be sent by invoking `handle_cmd` on the returned [EntityRef].
    pub async fn spawn(
        id: Uuid,
        mut event_sourced: E,
        mut evt_log: L,
    ) -> Result<EntityRef<E, L>, Error<E, L>> {
        debug!(%id, "Restoring entity");
        let to_seq_no = evt_log
            .last_seq_no(id)
            .await
            .map_err(|source| Error::Restore { id, source })?;
        if to_seq_no > 0 {
            let evts = evt_log
                .evts_by_id::<E::Evt>(id, 1, to_seq_no)
                .await
                .map_err(|source| Error::Restore { id, source })?;
            pin!(evts);
            while let Some(evt) = evts.next().await {
                let evt = evt.map_err(|source| Error::Restore { id, source })?;
                event_sourced.handle_evt(&evt);
            }
        }

        let mut entity = Entity {
            id,
            event_sourced,
            evt_log,
            seq_no: 0,
        };

        let (cmd_in, mut cmd_out) =
            mpsc::channel::<(E::Cmd, oneshot::Sender<Result<Vec<E::Evt>, E::Error>>)>(42);

        task::spawn(async move {
            while let Some((cmd, result_sender)) = cmd_out.recv().await {
                match entity.handle_cmd(cmd).await {
                    Ok((next_entity, result)) => {
                        entity = next_entity;
                        if let Err(error) = result_sender.send(result) {
                            error!(%id, ?error, "Entity cannot send command handler result to client");
                        };
                    }
                    Err(error) => {
                        error!(%id, %error, "Entity cannot persist events");
                        break;
                    }
                }
            }
            info!(%id, "Entity terminated");
        });

        Ok(EntityRef {
            id,
            cmd_in,
            _l: PhantomData,
        })
    }

    async fn handle_cmd(
        mut self,
        cmd: E::Cmd,
    ) -> Result<(Self, Result<Vec<E::Evt>, E::Error>), L::Error> {
        // TODO Remove this helper once the async in trait story is complete, also see below!
        fn send_fut<T>(
            f: impl std::future::Future<Output = Result<(), T>> + Send,
        ) -> impl std::future::Future<Output = Result<(), T>> + Send {
            f
        }

        // Handle command
        let evts = match self.event_sourced.handle_cmd(cmd) {
            Ok(evts) => evts,
            Err(error) => return Ok((self, Err(error))),
        };

        if !evts.is_empty() {
            // Persist events
            // self.evt_log.persist(self.id, &evts, self.seq_no).await?;
            // TODO Remove this helper once the async in trait story is complete, also see above!
            let send_fut = send_fut(self.evt_log.persist(self.id, &evts, self.seq_no));
            send_fut.await?;
            self.seq_no += evts.len() as u64;

            // Handle persisted events
            for evt in &evts {
                self.event_sourced.handle_evt(evt);
            }
        }

        Ok((self, Ok(evts)))
    }
}

/// A proxy to a spawned [Entity] which can be used to invoke its command handler.
#[derive(Debug, Clone)]
pub struct EntityRef<E, L>
where
    E: EventSourced,
    L: EvtLog,
{
    id: Uuid,
    cmd_in: mpsc::Sender<(E::Cmd, oneshot::Sender<Result<Vec<E::Evt>, E::Error>>)>,
    _l: PhantomData<L>,
}

impl<E, L> EntityRef<E, L>
where
    E: EventSourced + 'static,
    L: EvtLog + 'static,
{
    /// Invoke the command handler of the proxied [Entity].
    pub async fn handle_cmd(&self, cmd: E::Cmd) -> Result<Vec<E::Evt>, Error<E, L>> {
        let (result_in, result_out) = oneshot::channel();

        self.cmd_in
            .send((cmd, result_in))
            .await
            .map_err(|source| Error::Send {
                id: self.id,
                source,
            })?;

        result_out
            .await
            .map_err(|source| Error::Rcv {
                id: self.id,
                source,
            })?
            .map_err(|source| Error::CmdHandler {
                id: self.id,
                source,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::{stream, Stream};
    use serde::Deserialize;

    use thiserror::Error;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Cmd {
        Inc(u64),
        Dec(u64),
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    enum Evt {
        Increased { old_value: u64, inc: u64 },
        Decreased { old_value: u64, dec: u64 },
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
    enum Error {
        #[error("Overflow: value={value}, increment={inc}")]
        Overflow { value: u64, inc: u64 },
        #[error("Underflow: value={value}, decrement={dec}")]
        Underflow { value: u64, dec: u64 },
    }

    #[derive(Debug)]
    struct Counter(u64);

    impl EventSourced for Counter {
        type Cmd = Cmd;

        type Evt = Evt;

        type Error = Error;

        fn handle_cmd(&self, cmd: Self::Cmd) -> Result<Vec<Self::Evt>, Self::Error> {
            match cmd {
                Cmd::Inc(inc) => {
                    if inc > u64::MAX - self.0 {
                        Err(Error::Overflow { value: self.0, inc })
                    } else {
                        Ok(vec![Evt::Increased {
                            old_value: self.0,
                            inc,
                        }])
                    }
                }
                Cmd::Dec(dec) => {
                    if dec > self.0 {
                        Err(Error::Underflow { value: self.0, dec })
                    } else {
                        Ok(vec![Evt::Decreased {
                            old_value: self.0,
                            dec,
                        }])
                    }
                }
            }
        }

        fn handle_evt(&mut self, evt: &Self::Evt) {
            match evt {
                Evt::Increased { old_value: _, inc } => self.0 += inc,
                Evt::Decreased { old_value: _, dec } => self.0 -= dec,
            }
        }
    }

    #[derive(Debug, Clone)]
    struct NoopEvtLog;

    #[derive(Debug, Error)]
    #[error("NoopEvtLogError")]
    struct NoopEvtLogError;

    impl EvtLog for NoopEvtLog {
        type Error = NoopEvtLogError;

        async fn persist<'a, 'b, E>(
            &'a mut self,
            _entity_id: Uuid,
            _evts: &'b [E],
            _seq_no: u64,
        ) -> Result<(), Self::Error>
        where
            'b: 'a,
            E: Serialize + Send + Sync + 'a,
        {
            Ok(())
        }

        async fn last_seq_no(&self, _entity_id: Uuid) -> Result<u64, Self::Error> {
            Ok(1)
        }

        async fn evts_by_id<E>(
            &mut self,
            _entity_id: Uuid,
            _from_seq_no: u64,
            _to_seq_no: u64,
        ) -> Result<impl Stream<Item = Result<E, Self::Error>>, Self::Error>
        where
            E: DeserializeOwned,
        {
            Ok(stream::empty())
        }
    }

    #[tokio::test]
    async fn test() -> Result<(), Box<dyn std::error::Error>> {
        let event_log = NoopEvtLog;

        let entity = Entity::spawn(Uuid::now_v7(), Counter(0), event_log.clone()).await?;

        let evts = entity.handle_cmd(Cmd::Inc(42)).await;
        assert!(evts.is_ok());
        let evts = evts.unwrap();
        assert_eq!(
            evts,
            vec![Evt::Increased {
                old_value: 0,
                inc: 42,
            }]
        );

        let evts = entity.handle_cmd(Cmd::Dec(43)).await;
        assert!(evts.is_err());

        Ok(())
    }
}
