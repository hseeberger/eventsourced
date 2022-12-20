//! Event sourced entities.

#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]
#![feature(return_position_impl_trait_in_trait)]
#![allow(clippy::type_complexity)]

pub mod convert;
pub mod evt_log;
pub mod snapshot_store;

use crate::{
    convert::{TryFromBytes, TryIntoBytes},
    evt_log::EvtLog,
    snapshot_store::{Snapshot, SnapshotStore},
};
use futures::StreamExt;
use std::fmt::Debug;
use thiserror::Error;
use tokio::{
    pin,
    sync::{mpsc, oneshot},
    task,
};
use tracing::{debug, error};
use uuid::Uuid;

/// Command and event handling for an event sourced [Entity].
pub trait EventSourced {
    /// Command type.
    type Cmd: Debug;

    /// Event type.
    type Evt: Debug;

    /// Snapshot state type.
    type State;

    /// Error type for the command handler.
    type Error: std::error::Error;

    /// Command handler, returning the to be persisted events.
    fn handle_cmd(&self, cmd: Self::Cmd) -> Result<Vec<Self::Evt>, Self::Error>;

    /// Event handler, returning whether to take a snapshot or not.
    fn handle_evt(&mut self, seq_no: u64, evt: &Self::Evt) -> Option<Self::State>;

    /// Snapshot state handler.
    fn set_state(&mut self, state: Self::State);
}

/// An event sourced entity, uniquely identified by its ID.
///
/// Commands are handled by the command handler of its [EventSourced] value. Valid commands may
/// produce events which get persisted along with an increasing sequence number to its [EvtLog] and
/// then applied to the event handler of its [EventSourced] value. The event handler may decide to
/// save a snapshot at the current sequence number which is used to speed up spawning.
pub struct Entity<E, L, S> {
    id: Uuid,
    seq_no: u64,
    event_sourced: E,
    evt_log: L,
    snapshot_store: S,
}

impl<E, L, S> Entity<E, L, S>
where
    E: EventSourced + Send + 'static,
    E::Cmd: Send + 'static,
    E::Evt: TryIntoBytes + TryFromBytes + Send + Sync,
    E::State: TryIntoBytes + TryFromBytes + Send + Sync,
    E::Error: Send,
    L: EvtLog + Send + 'static,
    S: SnapshotStore + Send + 'static,
{
    /// Spawns an event sourced [Entity] with the given ID and creates an [EntityRef] for it.
    ///
    /// Commands can be sent by invoking `handle_cmd` on the returned [EntityRef] which uses a
    /// buffered channel with the given size.
    ///
    /// First the given [SnapshotStore] is used to find and possibly load a snapshot. Then the
    /// [EvtLog] is used to find the last sequence number and then to load any remaining events.
    pub async fn spawn(
        id: Uuid,
        mut event_sourced: E,
        buffer: usize,
        evt_log: L,
        snapshot_store: S,
    ) -> Result<EntityRef<E>, SpawnEntityError<L, S>> {
        assert!(buffer >= 1, "buffer must be positive");

        // Restore snapshot.
        let mut snapshot_seq_no = None;
        if let Some(Snapshot { seq_no, state }) = snapshot_store
            .load::<E::State>(id)
            .await
            .map_err(SpawnEntityError::LoadSnapshot)?
        {
            debug!(%id, seq_no, "Restoring snapshot");
            event_sourced.set_state(state);
            snapshot_seq_no = Some(seq_no);
        }
        let snapshot_seq_no = snapshot_seq_no.unwrap_or(0);

        // Replay latest events.
        let last_seq_no = evt_log
            .last_seq_no(id)
            .await
            .map_err(SpawnEntityError::LastSeqNo)?;
        assert!(
            snapshot_seq_no <= last_seq_no,
            "snapshot_seq_no must be less than or equal to last_seq_no"
        );
        if snapshot_seq_no < last_seq_no {
            let from_seq_no = snapshot_seq_no + 1;
            debug!(%id, from_seq_no, last_seq_no , "Replaying evts");
            let evts = evt_log
                .evts_by_id::<E::Evt>(id, from_seq_no, last_seq_no)
                .await
                .map_err(SpawnEntityError::EvtsById)?;
            pin!(evts);
            while let Some(evt) = evts.next().await {
                let (seq_no, evt) = evt.map_err(SpawnEntityError::NextEvt)?;
                event_sourced.handle_evt(seq_no, &evt);
            }
        }

        // Create entity.
        let mut entity = Entity {
            id,
            seq_no: last_seq_no,
            event_sourced,
            evt_log,
            snapshot_store,
        };
        debug!(%id, "Entity created");

        let (cmd_in, mut cmd_out) =
            mpsc::channel::<(E::Cmd, oneshot::Sender<Result<Vec<E::Evt>, E::Error>>)>(buffer);

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
            debug!(%id, "Entity terminated");
        });

        Ok(EntityRef { id, cmd_in })
    }

    async fn handle_cmd(
        mut self,
        cmd: E::Cmd,
    ) -> Result<(Self, Result<Vec<E::Evt>, E::Error>), Box<dyn std::error::Error>> {
        // TODO Remove this helper once async fn in trait is stable!
        fn make_send<T, E>(
            f: impl std::future::Future<Output = Result<T, E>> + Send,
        ) -> impl std::future::Future<Output = Result<T, E>> + Send {
            f
        }

        // Handle command
        let evts = match self.event_sourced.handle_cmd(cmd) {
            Ok(evts) => evts,
            Err(error) => return Ok((self, Err(error))),
        };

        if !evts.is_empty() {
            // Persist events
            // TODO Remove this helper once async fn in trait is stable!
            let send_fut = make_send(self.evt_log.persist(self.id, &evts, self.seq_no));
            send_fut.await?;

            // Handle persisted events
            for evt in &evts {
                self.seq_no += 1;
                if let Some(state) = self.event_sourced.handle_evt(self.seq_no, evt) {
                    debug!(id = %self.id, seq_no = self.seq_no, "Saving snapshot");
                    // TODO Remove this helper once async fn in trait is stable!
                    let send_fut =
                        make_send(self.snapshot_store.save(self.id, self.seq_no, &state));
                    send_fut.await?;
                }
            }
        }

        Ok((self, Ok(evts)))
    }
}

/// Errors from spawning an event sourced [Entity].
#[derive(Debug, Error)]
pub enum SpawnEntityError<L, S>
where
    L: EvtLog + 'static,
    S: SnapshotStore + 'static,
{
    /// A snapshot cannot be loaded from the snapshot store.
    #[error("Cannot load snapshot from snapshot store")]
    LoadSnapshot(#[source] S::Error),

    /// The last seqence number cannot be obtained from the event log.
    #[error("Cannot get last seqence number from event log")]
    LastSeqNo(#[source] L::Error),

    /// Events by ID cannot be obtained from the event log.
    #[error("Cannot get events by ID from event log")]
    EvtsById(#[source] L::Error),

    /// The next event cannot be obtained from the event log.
    #[error("Cannot get next event from event log")]
    NextEvt(#[source] L::Error),
}

/// A proxy to a spawned event sourced [Entity] which can be used to invoke its command handler.
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
    /// Get the ID of the proxied event sourced [Entity].
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Invoke the command handler of the proxied event sourced [Entity].
    pub async fn handle_cmd(&self, cmd: E::Cmd) -> Result<Vec<E::Evt>, EntityRefError<E>> {
        let (result_in, result_out) = oneshot::channel();
        self.cmd_in.send((cmd, result_in)).await?;
        result_out.await?.map_err(EntityRefError::InvalidCommand)
    }
}

/// Errors from an [EntityRef].
#[derive(Debug, Error)]
pub enum EntityRefError<E>
where
    E: EventSourced + 'static,
{
    /// An invalid command has been rejected by a command hander. This is considered a client
    /// error, like 400 Bad Request, i.e. normal behavior of the event sourced [Entity] and its
    /// [EntityRef].
    #[error("Invalid command rejected by command handler")]
    InvalidCommand(#[source] E::Error),

    /// A command cannot be sent from an [EntityRef] to its [Entity]. This is considered an
    /// internal error, like 500 Internal Server Error, i.e. erroneous behavior of the event
    /// sourced [Entity] and its [EntityRef].
    #[error("Cannot send command to Entity")]
    SendCmd(
        #[from] mpsc::error::SendError<(E::Cmd, oneshot::Sender<Result<Vec<E::Evt>, E::Error>>)>,
    ),

    /// An [EntityRef] cannot receive the command handler result from its [Entity], potentially
    /// because its entity has terminated. This is considered an internal error, like 500 Internal
    /// Server Error, i.e. erroneous behavior of the event sourced [Entity] and its [EntityRef].
    #[error("Cannot receive command handler result from Entity")]
    EntityTerminated(#[from] oneshot::error::RecvError),
}

#[cfg(all(test, feature = "serde_json"))]
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

        type State = u64;

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

        fn handle_evt(&mut self, _seq_no: u64, evt: &Self::Evt) -> Option<Self::State> {
            match evt {
                Evt::Increased { old_value: _, inc } => {
                    self.0 += inc;
                    None
                }
                Evt::Decreased { old_value: _, dec } => {
                    self.0 -= dec;
                    None
                }
            }
        }

        fn set_state(&mut self, state: Self::State) {
            self.0 = state;
        }
    }

    #[derive(Debug)]
    struct TestEvtLog;

    impl EvtLog for TestEvtLog {
        type Error = TestEvtLogError;

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
            Ok(42)
        }

        async fn evts_by_id<E>(
            &self,
            _entity_id: Uuid,
            _from_seq_no: u64,
            _to_seq_no: u64,
        ) -> Result<impl Stream<Item = Result<(u64, E), Self::Error>>, Self::Error>
        where
            E: TryFromBytes + Send,
        {
            Ok(stream::iter(0..42).map(|n| {
                let evt = Evt::Increased {
                    old_value: n,
                    inc: 1,
                };
                let bytes = serde_json::to_value(evt).unwrap().to_string().into();
                let evt = E::try_from_bytes(bytes).unwrap();
                Ok((n + 1, evt))
            }))
        }
    }

    #[derive(Debug, Error)]
    #[error("TestEvtLogError")]
    struct TestEvtLogError;

    #[derive(Debug)]
    struct TestSnapshotStore;

    impl SnapshotStore for TestSnapshotStore {
        type Error = TestSnapshotStoreError;

        async fn save<'a, 'b, S>(
            &'a mut self,
            _id: Uuid,
            _seq_no: u64,
            _state: &'b S,
        ) -> Result<(), Self::Error>
        where
            'b: 'a,
            S: TryIntoBytes + Send + Sync + 'a,
        {
            Ok(())
        }

        async fn load<S>(&self, _id: Uuid) -> Result<Option<Snapshot<S>>, Self::Error>
        where
            S: TryFromBytes,
        {
            let bytes = serde_json::to_value(42).unwrap().to_string().into();
            let state = S::try_from_bytes(bytes).unwrap();
            Ok(Some(Snapshot { seq_no: 42, state }))
        }
    }

    #[derive(Debug, Error)]
    #[error("TestSnapshotStoreError")]
    struct TestSnapshotStoreError;

    #[tokio::test]
    async fn test() -> Result<(), Box<dyn std::error::Error>> {
        let event_log = TestEvtLog;
        let snapshot_store = TestSnapshotStore;

        let entity =
            Entity::spawn(Uuid::now_v7(), Counter(0), 42, event_log, snapshot_store).await?;

        let evts = entity.handle_cmd(Cmd::Inc(1)).await?;
        assert_eq!(
            evts,
            vec![Evt::Increased {
                old_value: 42,
                inc: 1,
            }]
        );

        let evts = entity.handle_cmd(Cmd::Dec(666)).await;
        assert!(evts.is_err());

        Ok(())
    }
}
