//! Event sourced entities.

pub mod convert;
mod evt_log;
mod seq_no;
mod snapshot_store;
mod tagged_evt;

pub use evt_log::*;
pub use seq_no::*;
pub use snapshot_store::*;
pub use tagged_evt::*;

use bytes::Bytes;
use futures::StreamExt;
use std::{error::Error as StdError, fmt::Debug, num::NonZeroUsize};
use thiserror::Error;
use tokio::{
    pin,
    sync::{mpsc, oneshot},
    task,
};
use tracing::{debug, error};
use uuid::Uuid;

/// Command and event handling for an event sourced entity.
pub trait EventSourced: Sized + Send + 'static {
    /// Command type.
    type Cmd: Send;

    /// Event type.
    type Evt: Send;

    /// Snapshot state type.
    type State: Send;

    /// Error type for rejected (a.k.a. invalid) commands.
    type Error: StdError + Send;

    /// Command handler, returning the to be persisted event or an error.
    fn handle_cmd(&self, cmd: Self::Cmd) -> Result<impl IntoTaggedEvt<Self::Evt>, Self::Error>;

    /// Event handler, returning whether to take a snapshot or not.
    fn handle_evt(&mut self, evt: Self::Evt) -> Option<Self::State>;

    /// Snapshot state handler.
    fn set_state(&mut self, state: Self::State);
}

/// Extension methods for types implementing [EventSourced].
pub trait EventSourcedExt {
    /// Spawns an entity implementing [EventSourced] with the given ID and creates an [EntityRef]
    /// as a handle for it.
    ///
    /// First the given [SnapshotStore] is used to find and possibly load a snapshot. Then the
    /// [EvtLog] is used to find the last sequence number and then to load any remaining events.
    ///
    /// Commands can be passed to the spawned entity by invoking `handle_cmd` on the returned
    /// [EntityRef] which uses a buffered channel with the given size.
    ///
    /// Commands are handled by the command handler of the spawned entity. They can be rejected by
    /// returning an error. Valid commands produce an event with an optional tag which gets
    /// persisted to the [EvtLog] and then applied to the event handler of the respective
    /// entity. The event handler may decide to save a snapshot which is used to speed up future
    /// spawning.
    async fn spawn<
        L,
        S,
        EvtToBytes,
        EvtToBytesError,
        StateToBytes,
        StateToBytesError,
        EvtFromBytes,
        EvtFromBytesError,
        StateFromBytes,
        StateFromBytesError,
    >(
        mut self,
        id: Uuid,
        cmd_buffer: NonZeroUsize,
        evt_log: L,
        snapshot_store: S,
        binarizer: Binarizer<EvtToBytes, EvtFromBytes, StateToBytes, StateFromBytes>,
    ) -> Result<EntityRef<Self>, SpawnError>
    where
        Self: EventSourced,
        L: EvtLog,
        S: SnapshotStore,
        EvtToBytes: Fn(&Self::Evt) -> Result<Bytes, EvtToBytesError> + Send + Sync + 'static,
        EvtToBytesError: StdError + Send + Sync + 'static,
        StateToBytes: Fn(&Self::State) -> Result<Bytes, StateToBytesError> + Send + Sync + 'static,
        StateToBytesError: StdError + Send + Sync + 'static,
        EvtFromBytes:
            Fn(Bytes) -> Result<Self::Evt, EvtFromBytesError> + Copy + Send + Sync + 'static,
        EvtFromBytesError: StdError + Send + Sync + 'static,
        StateFromBytes:
            Fn(Bytes) -> Result<Self::State, StateFromBytesError> + Copy + Send + Sync + 'static,
        StateFromBytesError: StdError + Send + Sync + 'static,
    {
        let Binarizer {
            evt_to_bytes,
            evt_from_bytes,
            state_to_bytes,
            state_from_bytes,
        } = binarizer;

        // Restore snapshot.
        let snapshot_seq_no = snapshot_store
            .load::<Self::State, _, _>(id, state_from_bytes)
            .await
            .map_err(|source| SpawnError::LoadSnapshot(source.into()))?
            .map(|Snapshot { seq_no, state }| {
                debug!(%id, %seq_no, "restoring snapshot");
                self.set_state(state);
                seq_no
            });

        // Replay latest events.
        let last_seq_no = evt_log
            .last_seq_no(id)
            .await
            .map_err(|source| SpawnError::LastSeqNo(source.into()))?;
        assert!(
            snapshot_seq_no <= last_seq_no,
            "snapshot_seq_no must be less than or equal to last_seq_no"
        );
        if snapshot_seq_no < last_seq_no {
            let from_seq_no = snapshot_seq_no.unwrap_or(SeqNo::MIN);
            let to_seq_no = last_seq_no.unwrap_or(SeqNo::MIN);
            debug!(%id, %from_seq_no, %to_seq_no , "replaying evts");
            let evts = evt_log
                .evts_by_id::<Self::Evt, _, _>(id, from_seq_no, evt_from_bytes)
                .await
                .map_err(|source| SpawnError::EvtsById(source.into()))?;
            pin!(evts);
            while let Some(evt) = evts.next().await {
                let (seq_no, evt) = evt.map_err(|source| SpawnError::NextEvt(source.into()))?;
                self.handle_evt(evt);
                if seq_no == to_seq_no {
                    break;
                }
            }
        }

        // Create entity.
        let mut entity = Entity {
            event_sourced: self,
            id,
            evt_log,
            snapshot_store,
            evt_to_bytes,
            state_to_bytes,
        };
        debug!(%id, "entity created");

        let (cmd_in, mut cmd_out) = mpsc::channel::<(
            Self::Cmd,
            oneshot::Sender<Result<(), Self::Error>>,
        )>(cmd_buffer.get());

        // Spawn handler loop.
        task::spawn(async move {
            while let Some((cmd, result_sender)) = cmd_out.recv().await {
                match entity.handle_cmd(cmd).await {
                    Ok(result) => {
                        if result_sender.send(result).is_err() {
                            error!(%id, "cannot send command handler result");
                        };
                    }
                    Err(error) => {
                        error!(%id, %error, "cannot persist event");
                        break;
                    }
                }
            }
            debug!(%id, "entity terminated");
        });

        Ok(EntityRef { id, cmd_in })
    }
}

impl<E> EventSourcedExt for E where E: EventSourced {}

/// Error from spawning an event sourced entity.
#[derive(Debug, Error)]
pub enum SpawnError {
    /// A snapshot cannot be loaded from the snapshot store.
    #[error("cannot load snapshot from snapshot store")]
    LoadSnapshot(#[source] Box<dyn StdError>),

    /// The last seqence number cannot be obtained from the event log.
    #[error("cannot get last seqence number from event log")]
    LastSeqNo(#[source] Box<dyn StdError>),

    /// Events by ID cannot be obtained from the event log.
    #[error("cannot get events by ID from event log")]
    EvtsById(#[source] Box<dyn StdError>),

    /// The next event cannot be obtained from the event log.
    #[error("cannot get next event from event log")]
    NextEvt(#[source] Box<dyn StdError>),
}

/// A handle for a spawned [EventSourced] entity which can be used to invoke its command handler.
#[derive(Debug, Clone)]
pub struct EntityRef<E>
where
    E: EventSourced,
{
    id: Uuid,
    cmd_in: mpsc::Sender<(E::Cmd, oneshot::Sender<Result<(), E::Error>>)>,
}

impl<E> EntityRef<E>
where
    E: EventSourced,
{
    /// Get the ID of the proxied event sourced entity.
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Invoke the command handler of the entity.
    ///
    /// The returned (outer) `Result` signals, whether the command could be sent to the entity and
    /// the command handler result could be received, i.e. an `Err` signals a technical failure.
    ///
    /// The (outer) `Ok` variant contains another (inner) `Result`, which signals whether the
    /// command was valid or rejected. If it was valid, the persisted event is returned, else the
    /// rejection error.
    pub async fn handle_cmd(&self, cmd: E::Cmd) -> Result<Result<(), E::Error>, EntityRefError> {
        let (result_in, result_out) = oneshot::channel();
        self.cmd_in
            .send((cmd, result_in))
            .await
            .map_err(|source| EntityRefError::SendCmd(source.into()))?;
        result_out.await.map_err(EntityRefError::RcvHandlerResult)
    }
}

/// Error from an [EntityRef].
#[derive(Debug, Error)]
pub enum EntityRefError {
    /// A command cannot be sent from an [EntityRef] to its entity.
    #[error("cannot send command to Entity")]
    SendCmd(#[source] Box<dyn StdError>),

    /// An [EntityRef] cannot receive the command handler result from its entity, potentially
    /// because its entity has terminated.
    #[error("cannot receive command handler result from Entity")]
    RcvHandlerResult(#[from] oneshot::error::RecvError),
}

/// Collection of conversion functions from and to [Bytes](bytes::Bytes) for events and snapshots.
pub struct Binarizer<EvtToBytes, EvtFromBytes, StateToBytes, StateFromBytes> {
    pub evt_to_bytes: EvtToBytes,
    pub evt_from_bytes: EvtFromBytes,
    pub state_to_bytes: StateToBytes,
    pub state_from_bytes: StateFromBytes,
}

struct Entity<E, L, S, EvtToBytes, StateToBytes> {
    event_sourced: E,
    id: Uuid,
    evt_log: L,
    snapshot_store: S,
    evt_to_bytes: EvtToBytes,
    state_to_bytes: StateToBytes,
}

impl<E, L, S, EvtToBytes, EvtToBytesError, StateToBytes, StateToBytesError>
    Entity<E, L, S, EvtToBytes, StateToBytes>
where
    E: EventSourced,
    L: EvtLog,
    S: SnapshotStore,
    EvtToBytes: Fn(&E::Evt) -> Result<Bytes, EvtToBytesError> + Send + Sync + 'static,
    EvtToBytesError: StdError + Send + Sync + 'static,
    StateToBytes: Fn(&E::State) -> Result<Bytes, StateToBytesError> + Send + Sync + 'static,
    StateToBytesError: StdError + Send + Sync + 'static,
{
    async fn handle_cmd(&mut self, cmd: E::Cmd) -> Result<Result<(), E::Error>, Box<dyn StdError>> {
        let (seq_no, evt) = match self.event_sourced.handle_cmd(cmd) {
            Ok(tagged_evt) => {
                let TaggedEvt { evt, tag } = tagged_evt.into_tagged_evt();
                let seq_no = self
                    .evt_log
                    .persist(&evt, tag, self.id, &self.evt_to_bytes)
                    .await?;
                (seq_no, evt)
            }

            Err(error) => return Ok(Err(error)),
        };

        let state = self.event_sourced.handle_evt(evt);

        // Persist latest snapshot if any.
        if let Some(state) = state {
            debug!(id = %self.id, %seq_no, "Saving snapshot");
            self.snapshot_store
                .save(self.id, seq_no, state, &self.state_to_bytes)
                .await?;
        }

        Ok(Ok(()))
    }
}

#[cfg(all(test, feature = "prost"))]
mod tests {
    use super::*;
    use async_stream::stream;
    use bytes::BytesMut;
    use futures::{stream, Stream};
    use prost::Message;
    use std::convert::Infallible;

    #[derive(Debug)]
    struct Simple(u64);

    impl EventSourced for Simple {
        type Cmd = ();

        type Evt = u64;

        type State = u64;

        type Error = Infallible;

        fn handle_cmd(
            &self,
            _cmd: Self::Cmd,
        ) -> Result<impl IntoTaggedEvt<Self::Evt>, Self::Error> {
            Ok(((1 << 32) + self.0).with_tag("tag"))
        }

        fn handle_evt(&mut self, evt: Self::Evt) -> Option<Self::State> {
            self.0 += evt >> 32;
            None
        }

        fn set_state(&mut self, state: Self::State) {
            self.0 = state;
        }
    }

    #[derive(Debug, Clone)]
    struct TestEvtLog;

    impl EvtLog for TestEvtLog {
        type Error = TestEvtLogError;

        async fn persist<'a, E, ToBytes, ToBytesError>(
            &'a mut self,
            _id: Uuid,
            _evt: &'a E,
            _tag: Option<String>,
            _to_bytes: &'a ToBytes,
        ) -> Result<SeqNo, Self::Error>
        where
            E: Send + Sync + 'a,
            ToBytes: Fn(&E) -> Result<Bytes, ToBytesError> + Send + Sync,
            ToBytesError: StdError + Send + Sync + 'static,
        {
            Ok(SeqNo(43.try_into().unwrap()))
        }

        async fn last_seq_no(&self, _entity_id: Uuid) -> Result<Option<SeqNo>, Self::Error> {
            Ok(Some(SeqNo(42.try_into().unwrap())))
        }

        async fn evts_by_id<'a, E, EvtFromBytes, EvtFromBytesError>(
            &'a self,
            _id: Uuid,
            from_seq_no: SeqNo,
            evt_from_bytes: EvtFromBytes,
        ) -> Result<impl Stream<Item = Result<(SeqNo, E), Self::Error>> + Send, Self::Error>
        where
            E: Send + 'a,
            EvtFromBytes: Fn(Bytes) -> Result<E, EvtFromBytesError> + Copy + Send + Sync + 'static,
            EvtFromBytesError: StdError + Send + Sync + 'static,
        {
            let evts = stream! {
                for n in 0..666 {
                    for evt in 1..=3 {
                        let seq_no = (n * 3 + evt).try_into().unwrap();
                        if from_seq_no <= seq_no  {
                            let mut bytes = BytesMut::new();
                            evt.encode(&mut bytes).map_err(|source| TestEvtLogError(source.into()))?;
                            let evt = evt_from_bytes(bytes.into()).map_err(|source| TestEvtLogError(source.into()))?;
                            yield Ok((seq_no, evt));
                        }
                    }

                }
            };
            Ok(evts)
        }

        async fn evts_by_tag<'a, E, T, EvtFromBytes, EvtFromBytesError>(
            &'a self,
            _tag: T,
            _from_seq_no: SeqNo,
            _evt_from_bytes: EvtFromBytes,
        ) -> Result<impl Stream<Item = Result<(SeqNo, E), Self::Error>> + Send, Self::Error>
        where
            E: Send + 'a,
            EvtFromBytes: Fn(Bytes) -> Result<E, EvtFromBytesError> + Copy + Send + Sync + 'static,
            EvtFromBytesError: StdError + Send + Sync + 'static,
            T: Into<String> + Send,
        {
            Ok(stream::empty())
        }
    }

    #[derive(Debug, Error)]
    #[error("TestEvtLogError")]
    struct TestEvtLogError(#[source] Box<dyn StdError + Send + Sync>);

    #[derive(Debug, Clone)]
    struct TestSnapshotStore;

    impl SnapshotStore for TestSnapshotStore {
        type Error = TestSnapshotStoreError;

        async fn save<'a, S, StateToBytes, StateToBytesError>(
            &'a mut self,
            _id: Uuid,
            _seq_no: SeqNo,
            _state: S,
            _state_to_bytes: &'a StateToBytes,
        ) -> Result<(), Self::Error>
        where
            S: Send + Sync + 'a,
            StateToBytes: Fn(&S) -> Result<Bytes, StateToBytesError> + Send + Sync + 'static,
            StateToBytesError: StdError + Send + Sync + 'static,
        {
            Ok(())
        }

        async fn load<'a, S, StateFromBytes, StateFromBytesError>(
            &'a self,
            _id: Uuid,
            state_from_bytes: StateFromBytes,
        ) -> Result<Option<Snapshot<S>>, Self::Error>
        where
            S: 'a,
            StateFromBytes:
                Fn(Bytes) -> Result<S, StateFromBytesError> + Copy + Send + Sync + 'static,
            StateFromBytesError: StdError + Send + Sync + 'static,
        {
            let mut bytes = BytesMut::new();
            42.encode(&mut bytes).unwrap();
            let state = state_from_bytes(bytes.into()).unwrap();
            Ok(Some(Snapshot {
                seq_no: 42.try_into().unwrap(),
                state,
            }))
        }
    }

    #[derive(Debug, Error)]
    #[error("TestSnapshotStoreError")]
    struct TestSnapshotStoreError;

    #[tokio::test]
    async fn test_spawn_handle_cmd() -> Result<(), Box<dyn StdError>> {
        let evt_log = TestEvtLog;
        let snapshot_store = TestSnapshotStore;

        let entity = spawn(evt_log, snapshot_store).await?;
        entity.handle_cmd(()).await??;

        Ok(())
    }

    // We go through these hoops to ensure oddities in "async fn in trait" and other unstable
    // features are handle appropriately, e.g. by asserting futures are send.
    async fn spawn<E, S>(
        evt_log: E,
        snapshot_store: S,
    ) -> Result<EntityRef<Simple>, Box<dyn StdError>>
    where
        E: EvtLog,
        S: SnapshotStore,
    {
        let entity = task::spawn(async move {
            Simple(0)
                .spawn(
                    Uuid::now_v7(),
                    unsafe { NonZeroUsize::new_unchecked(1) },
                    evt_log,
                    snapshot_store,
                    convert::prost::binarizer(),
                )
                .await
        });

        let entity = entity.await??;
        Ok(entity)
    }
}
