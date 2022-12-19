//! Event sourced entities.

#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]
#![feature(return_position_impl_trait_in_trait)]
#![allow(clippy::type_complexity)]

pub mod convert;
pub mod evt_log;

use convert::{TryFromBytes, TryIntoBytes};
use evt_log::EvtLog;
use futures::StreamExt;
use std::fmt::Debug;
use thiserror::Error;
use tokio::{
    pin,
    sync::{mpsc, oneshot},
    task,
};
use tracing::{debug, error, info};
use uuid::Uuid;

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
    E::Evt: TryIntoBytes + TryFromBytes + Send + Sync,
    E::Error: Send,
    L: EvtLog + Send + 'static,
{
    /// Create an [Entity] with the given ID, [EventSourced] command/event handling and [EvtLog].
    /// Commands can be sent by invoking `handle_cmd` on the returned [EntityRef].
    pub async fn spawn(
        id: Uuid,
        mut event_sourced: E,
        mut evt_log: L,
    ) -> Result<EntityRef<E>, L::Error> {
        // Create entity, possibly by replaying existing events.
        let last_seq_no = evt_log.last_seq_no(id).await?;
        if last_seq_no > 0 {
            let evts = evt_log.evts_by_id::<E::Evt>(id, 1, last_seq_no).await?;
            pin!(evts);
            while let Some(evt) = evts.next().await {
                event_sourced.handle_evt(&evt?);
            }
        }
        let mut entity = Entity {
            id,
            event_sourced,
            evt_log,
            seq_no: last_seq_no,
        };
        debug!(%id, %last_seq_no, "Created entity");

        // TODO Make EntityRef channel buffer size configurable!
        let (cmd_in, mut cmd_out) =
            mpsc::channel::<(E::Cmd, oneshot::Sender<Result<Vec<E::Evt>, E::Error>>)>(42);

        // Spawn handler loop.
        task::spawn(async move {
            while let Some((cmd, result_sender)) = cmd_out.recv().await {
                match entity.handle_cmd(cmd).await {
                    Ok((next_entity, result)) => {
                        entity = next_entity;
                        if result_sender.send(result).is_err() {
                            error!(%id, "Cannot send command handler result");
                        };
                    }
                    Err(error) => {
                        error!(%id, %error, "Cannot persist events");
                        break;
                    }
                }
            }
            info!(%id, "Entity terminated");
        });

        Ok(EntityRef { id, cmd_in })
    }

    async fn handle_cmd(
        mut self,
        cmd: E::Cmd,
    ) -> Result<(Self, Result<Vec<E::Evt>, E::Error>), L::Error> {
        // TODO Remove this helper once the async in trait story is complete, also see below!
        fn make_send<T>(
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
            let send_fut = make_send(self.evt_log.persist(self.id, &evts, self.seq_no));
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
pub struct EntityRef<E>
where
    E: EventSourced,
{
    id: Uuid,
    cmd_in: mpsc::Sender<(E::Cmd, oneshot::Sender<Result<Vec<E::Evt>, E::Error>>)>,
}

impl<E> EntityRef<E>
where
    E: EventSourced + 'static,
{
    /// Get the ID of the proxied [Entity].
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Invoke the command handler of the proxied [Entity].
    pub async fn handle_cmd(&self, cmd: E::Cmd) -> Result<Vec<E::Evt>, Error<E>> {
        let (result_in, result_out) = oneshot::channel();
        self.cmd_in.send((cmd, result_in)).await?;
        result_out.await?.map_err(Error::InvalidCommand)
    }
}

/// Errors from an [EntityRef].
#[derive(Debug, Error)]
pub enum Error<E>
where
    E: EventSourced + 'static,
{
    /// A command cannot be sent from an [EntityRef] to its [Entity].
    #[error("Cannot send command from EntityRef to Entity")]
    SendCmd(
        #[from] mpsc::error::SendError<(E::Cmd, oneshot::Sender<Result<Vec<E::Evt>, E::Error>>)>,
    ),

    /// An [EntityRef] cannot receive the command handler result, because its entity has
    /// terminated.
    #[error("Cannot receive command handler result, because entity has terminated")]
    EntityTerminated(#[from] oneshot::error::RecvError),

    /// An invalid command has been rejected by a command hander.
    #[error("Invalid command rejected by command handler")]
    InvalidCommand(#[source] E::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{stream, Stream};
    use serde::{Deserialize, Serialize};
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
            E: TryIntoBytes + Send + Sync + 'a,
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
            E: TryFromBytes + Send,
        {
            Ok(stream::empty())
        }
    }

    #[tokio::test]
    async fn test() -> Result<(), Box<dyn std::error::Error>> {
        let event_log = NoopEvtLog;

        let entity = Entity::spawn(Uuid::now_v7(), Counter(0), event_log.clone()).await?;

        let evts = entity.handle_cmd(Cmd::Inc(42)).await?;
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
