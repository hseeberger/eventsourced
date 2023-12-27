//! Event sourced entities.
//!
//! EventSourced is inspired to a large degree by the amazing
//! [Akka Persistence](https://doc.akka.io/docs/akka/current/typed/index-persistence.html) library.
//! It provides a framework for implementing
//! [Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html) and
//! [CQRS](https://www.martinfowler.com/bliki/CQRS.html).
//!
//! The [EvtLog] and [SnapshotStore] traits define a pluggable event log and a pluggable snapshot
//! store respectively. For [NATS](https://nats.io/) and [Postgres](https://www.postgresql.org/)
//! these are implemented in the respective crates.
//!
//! The [spawn](EventSourcedExt::spawn) function provides for creating event sourced entities,
//! identifiable by an ID, for some event log and  some snapshot store. Conversion of events and
//! snapshot state to and from bytes happens via given [Binarizer] functions; for
//! [prost](https://github.com/tokio-rs/prost) and
//! [serde_json](https://github.com/serde-rs/json) these are already provided.
//!
//! Calling [spawn](EventSourcedExt::spawn) results in a cloneable [EntityRef] which can be used to
//! pass commands to the spawned entity by invoking [handle_cmd](EntityRef::handle_cmd). Commands
//! are handled by the command handler of the spawned entity. They can be rejected by returning an
//! error. Valid commands produce an event with an optional tag which gets persisted to the [EvtLog]
//! and then applied to the event handler of the respective entity. The event handler may decide to
//! save a snapshot which is used to speed up future spawning.
//!
//! Events can be queried from the event log by ID or by tag. These queries can be used to build
//! read side projections.

pub mod convert;

mod evt_log;
mod snapshot_store;

pub use evt_log::*;
pub use snapshot_store::*;

use bytes::Bytes;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::{
    error::Error as StdError,
    fmt::Debug,
    num::{NonZeroU64, NonZeroUsize},
};
use thiserror::Error;
use tokio::{
    pin,
    sync::{mpsc, oneshot},
    task,
};
use tracing::{debug, error, instrument};

/// Command and event handling for an event sourced entity.
pub trait EventSourced {
    /// Id type.
    type Id: Debug + Send + 'static;

    /// Command type.
    type Cmd: Debug + Send + Sync + 'static;

    /// Event type.
    type Evt: Debug + Send + Sync;

    /// State type.
    type State: Debug + Default + Send + Sync + 'static;

    /// Error type for rejected (a.k.a. invalid) commands.
    type Error: StdError + Send + Sync + 'static;

    const TYPE_NAME: &'static str;

    /// Command handler, returning the to be persisted event or an error.
    fn handle_cmd(
        id: &Self::Id,
        state: &Self::State,
        cmd: Self::Cmd,
    ) -> Result<Self::Evt, Self::Error>;

    /// Event handler.
    fn handle_evt(state: Self::State, evt: Self::Evt) -> Self::State;
}

/// Extension methods for types implementing [EventSourced].
pub trait EventSourcedExt: Sized {
    /// Spawns an event sourced entity and creates an [EntityRef] as a handle for it.
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
    #[allow(async_fn_in_trait)]
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip(evt_log, snapshot_store, binarizer))]
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
        id: Self::Id,
        snapshot_after: Option<NonZeroU64>,
        cmd_buffer: NonZeroUsize,
        mut evt_log: L,
        mut snapshot_store: S,
        binarizer: Binarizer<EvtToBytes, EvtFromBytes, StateToBytes, StateFromBytes>,
    ) -> Result<EntityRef<Self>, SpawnError>
    where
        Self: EventSourced,
        L: EvtLog<Id = Self::Id>,
        S: SnapshotStore<Id = Self::Id>,
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
        let (snapshot_seq_no, state) = snapshot_store
            .load::<Self::State, _, _>(&id, state_from_bytes)
            .await
            .map_err(|error| SpawnError::LoadSnapshot(error.into()))?
            .map(|Snapshot { seq_no, state }| {
                debug!(?id, %seq_no, "restored snapshot");
                (seq_no, state)
            })
            .unzip();

        let mut state = state.unwrap_or_default();

        // Replay latest events.
        let mut last_seq_no = evt_log
            .last_seq_no(Self::TYPE_NAME, &id)
            .await
            .map_err(|error| SpawnError::LastNonZeroU64(error.into()))?;
        assert!(
            snapshot_seq_no <= last_seq_no,
            "snapshot_seq_no must be less than or equal to last_seq_no"
        );
        if snapshot_seq_no < last_seq_no {
            let from_seq_no = snapshot_seq_no.unwrap_or(NonZeroU64::MIN);
            let to_seq_no = last_seq_no.unwrap_or(NonZeroU64::MIN);
            debug!(?id, %from_seq_no, %to_seq_no , "replaying evts");
            let evts = evt_log
                .evts_by_id::<Self::Evt, _, _>(Self::TYPE_NAME, &id, from_seq_no, evt_from_bytes)
                .await
                .map_err(|error| SpawnError::EvtsById(error.into()))?;
            pin!(evts);
            while let Some(evt) = evts.next().await {
                let (seq_no, evt) = evt.map_err(|error| SpawnError::NextEvt(error.into()))?;
                state = Self::handle_evt(state, evt);
                if seq_no == to_seq_no {
                    break;
                }
            }
            debug!(?id, ?state, "replayed evts");
        }

        let mut evt_count = 0u64;
        let (cmd_in, mut cmd_out) = mpsc::channel::<(
            Self::Cmd,
            oneshot::Sender<Result<(), Self::Error>>,
        )>(cmd_buffer.get());

        // Spawn handler loop.
        task::spawn(async move {
            while let Some((cmd, result_sender)) = cmd_out.recv().await {
                debug!(?id, ?cmd, "handling command");
                let result = Self::handle_cmd(&id, &state, cmd);
                debug!(?id, "handled command");

                // This ugliness seems to be needed unfortunately, because matching on result
                // prevents from using `state` below, because would still be
                // borrowed.
                if let Err(error) = result {
                    if result_sender.send(Err(error)).is_err() {
                        error!(?id, "cannot send command handler result");
                    };
                    continue;
                };
                let evt = result.unwrap();

                debug!(?id, ?evt, "persisting event");
                match evt_log
                    .persist(&evt, Self::TYPE_NAME, &id, last_seq_no, &evt_to_bytes)
                    .await
                {
                    Ok(seq_no) => {
                        debug!(?id, ?evt, "persited event");
                        last_seq_no = Some(seq_no);
                        state = Self::handle_evt(state, evt);

                        evt_count += 1;
                        if snapshot_after
                            .map(|a| evt_count % a == 0)
                            .unwrap_or_default()
                        {
                            debug!(?id, ?seq_no, "saving snapshot");
                            if let Err(error) = snapshot_store
                                .save(&id, seq_no, &state, &state_to_bytes)
                                .await
                            {
                                error!(?id, %error, "cannot save snapshot");
                            };
                        }

                        if result_sender.send(Ok(())).is_err() {
                            error!(?id, "cannot send command handler result");
                        };
                    }

                    Err(error) => {
                        error!(?id, %error, "cannot persist event");
                        break;
                    }
                }
            }

            debug!(?id, "entity terminated");
        });

        Ok(EntityRef { cmd_in })
    }
}

/// Error from spawning an event sourced entity.
#[derive(Debug, Error)]
pub enum SpawnError {
    /// A snapshot cannot be loaded from the snapshot store.
    #[error("cannot load snapshot from snapshot store")]
    LoadSnapshot(#[source] Box<dyn StdError + Send + Sync>),

    /// The last seqence number cannot be obtained from the event log.
    #[error("cannot get last seqence number from event log")]
    LastNonZeroU64(#[source] Box<dyn StdError + Send + Sync>),

    /// Events by ID cannot be obtained from the event log.
    #[error("cannot get events by ID from event log")]
    EvtsById(#[source] Box<dyn StdError + Send + Sync>),

    /// The next event cannot be obtained from the event log.
    #[error("cannot get next event from event log")]
    NextEvt(#[source] Box<dyn StdError + Send + Sync>),
}

impl<E> EventSourcedExt for E where E: EventSourced {}

/// A handle for a spawned event sourced entity which can be used to invoke its command handler.
#[derive(Debug, Clone)]
#[allow(clippy::type_complexity)]
pub struct EntityRef<E>
where
    E: EventSourced,
{
    cmd_in: mpsc::Sender<(E::Cmd, oneshot::Sender<Result<(), E::Error>>)>,
}

impl<E> EntityRef<E>
where
    E: EventSourced,
{
    /// Invoke the command handler of the entity.
    #[instrument(skip(self))]
    pub async fn handle_cmd(&self, cmd: E::Cmd) -> Result<(), HandleCmdError<E>> {
        let (result_in, result_out) = oneshot::channel();
        self.cmd_in
            .send((cmd, result_in))
            .await
            .map_err(|_| HandleCmdError::Internal("cannot send command".to_string()))?;
        result_out
            .await
            .map_err(|_| {
                HandleCmdError::Internal("cannot receive command handler result".to_string())
            })?
            .map_err(HandleCmdError::Handler)
    }
}

/// Error from an [EntityRef].
#[derive(Debug, Error, Serialize, Deserialize)]
pub enum HandleCmdError<E>
where
    E: EventSourced,
{
    /// A command cannot be sent from an [EntityRef] to its entity or the result cannot be received
    /// from its entity.
    #[error("{0}")]
    Internal(String),

    /// Command handler result.
    #[error(transparent)]
    Handler(E::Error),
}

/// Collection of conversion functions from and to [Bytes] for events and snapshots.
pub struct Binarizer<EvtToBytes, EvtFromBytes, StateToBytes, StateFromBytes> {
    pub evt_to_bytes: EvtToBytes,
    pub evt_from_bytes: EvtFromBytes,
    pub state_to_bytes: StateToBytes,
    pub state_from_bytes: StateFromBytes,
}

#[cfg(all(test, feature = "prost"))]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use futures::{stream, Stream};
    use prost::Message;
    use std::convert::Infallible;
    use uuid::Uuid;

    #[derive(Debug)]
    struct Simple;

    impl EventSourced for Simple {
        type Id = Uuid;
        type Cmd = ();
        type Evt = u64;
        type State = u64;
        type Error = Infallible;

        const TYPE_NAME: &'static str = "simple";

        fn handle_cmd(
            _id: &Self::Id,
            state: &Self::State,
            _cmd: Self::Cmd,
        ) -> Result<Self::Evt, Self::Error> {
            Ok((1 << 32) + *state)
        }

        fn handle_evt(mut state: Self::State, evt: Self::Evt) -> Self::State {
            state += evt >> 32;
            state
        }
    }

    #[derive(Debug, Clone)]
    struct TestEvtLog;

    impl EvtLog for TestEvtLog {
        type Id = Uuid;
        type Error = TestEvtLogError;

        async fn persist<E, ToBytes, ToBytesError>(
            &mut self,
            _evt: &E,
            _type: &str,
            _id: &Self::Id,
            _last_seq_no: Option<NonZeroU64>,
            _to_bytes: &ToBytes,
        ) -> Result<NonZeroU64, Self::Error>
        where
            E: Sync,
            ToBytes: Fn(&E) -> Result<Bytes, ToBytesError> + Sync,
            ToBytesError: StdError + Send + Sync + 'static,
        {
            Ok(43.try_into().unwrap())
        }

        async fn last_seq_no(
            &self,
            _type: &str,
            _entity_id: &Self::Id,
        ) -> Result<Option<NonZeroU64>, Self::Error> {
            Ok(Some(42.try_into().unwrap()))
        }

        async fn evts_by_id<E, FromBytes, FromBytesError>(
            &self,
            _type: &str,
            _id: &Self::Id,
            _from_seq_no: NonZeroU64,
            _evt_from_bytes: FromBytes,
        ) -> Result<impl Stream<Item = Result<(NonZeroU64, E), Self::Error>> + Send, Self::Error>
        where
            E: Send,
            FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Copy + Send,
            FromBytesError: StdError + Send + Sync + 'static,
        {
            Ok(stream::empty())
        }

        async fn evts_by_type<E, FromBytes, FromBytesError>(
            &self,
            _type: &str,
            _from_seq_no: NonZeroU64,
            _evt_from_bytes: FromBytes,
        ) -> Result<impl Stream<Item = Result<(NonZeroU64, E), Self::Error>> + Send, Self::Error>
        where
            E: Send,
            FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Copy + Send,
            FromBytesError: StdError + Send + Sync + 'static,
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
        type Id = Uuid;
        type Error = TestSnapshotStoreError;

        async fn save<S, ToBytes, ToBytesError>(
            &mut self,
            _id: &Self::Id,
            _seq_no: NonZeroU64,
            _state: &S,
            _state_to_bytes: &ToBytes,
        ) -> Result<(), Self::Error>
        where
            S: Send,
            ToBytes: Fn(&S) -> Result<Bytes, ToBytesError> + Sync,
            ToBytesError: StdError,
        {
            Ok(())
        }

        async fn load<S, FromBytes, FromBytesError>(
            &self,
            _id: &Self::Id,
            state_from_bytes: FromBytes,
        ) -> Result<Option<Snapshot<S>>, Self::Error>
        where
            FromBytes: Fn(Bytes) -> Result<S, FromBytesError>,
            FromBytesError: StdError,
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

        let entity = Simple::spawn(
            Uuid::from_u128(1),
            None,
            unsafe { NonZeroUsize::new_unchecked(1) },
            evt_log,
            snapshot_store,
            convert::prost::binarizer(),
        )
        .await?;

        entity.handle_cmd(()).await?;

        Ok(())
    }
}
