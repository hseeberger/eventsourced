#![cfg_attr(docsrs, feature(doc_cfg))]

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
//! The [EventSourced] trait defines the event type and handling for event sourced entities. These
//! are identifiable by a type name and ID and can be created with the [EventSourcedExt::entity]
//! extension method. Commands can be registered with the [EventSourcedEntity::cmd] method where
//! the [Cmd] trait defines a command handler function to either reject a command or return an
//! event. An event gets persisted to the event log and then applied to the event handler to return
//! the new state of the entity.
//!
//! [EventSourcedEntity::spawn] puts the event sourced entity on the given event log and snapshot
//! store, returning an [EntityRef] which can be cheaply cloned and used to pass commands to the
//! entity. Conversion of events and snapshot state to and from bytes happens via the given
//! [Binarize] implementation; for [prost](https://github.com/tokio-rs/prost) and [serde_json](https://github.com/serde-rs/json)
//! these are already provided. Snapshots are taken after the configured number of processed events
//! to speed up future spawning.
//!
//! [EntityRef::handle_cmd] either returns [Cmd::Error] for a rejected command or [Cmd::Reply] for
//! an accepted one.
//!
//! Events can be queried from the event log by ID or by entity type. These queries can be used to
//! build read side projections. There is early support for projections in the
//! `eventsourced-projection` crate.

pub mod binarize;

mod evt_log;
mod snapshot_store;
mod util;

pub use evt_log::*;
pub use snapshot_store::*;

use crate::{binarize::Binarize, util::StreamExt as ThisStreamExt};
use error_ext::{BoxError, StdErrorExt};
use frunk::{
    coproduct::{CNil, CoproductSelector},
    Coprod,
};
use futures::{future::ok, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::{
    any::{Any, TypeId},
    collections::HashMap,
    fmt::Debug,
    marker::PhantomData,
    num::{NonZeroU64, NonZeroUsize},
};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot},
    task,
};
use tracing::{debug, error, instrument};

type BoxedAny = Box<dyn Any + Send>;
type BoxedHandleCmd<E> = Box<
    dyn Fn(&BoxedAny, &<E as EventSourced>::Id, &E) -> Result<<E as EventSourced>::Evt, BoxedAny>
        + Send,
>;
type BoxedReply<E> = Box<dyn Fn(BoxedAny, &E) -> BoxedAny + Send + Sync>;
type BoxedCmdFns<E> = HashMap<TypeId, (BoxedHandleCmd<E>, BoxedReply<E>)>;

/// The state of an event sourced entity as well as its event handling (which transforms the state).
pub trait EventSourced {
    /// The Id type.
    type Id;

    /// The event type.
    type Evt;

    /// The event handler.
    fn handle_evt(self, evt: Self::Evt) -> Self;
}

/// A command for the given [EventSourced] implementation, defining its handling and replying.
pub trait Cmd<E>
where
    Self: 'static,
    E: EventSourced,
{
    /// The type for rejecting this command.
    type Error: Send + 'static;

    /// The type for replies.
    type Reply: Send + 'static;

    /// The command handler, taking this command, and references to the ID and the state of
    /// the event sourced entity, either rejecting this command via [Self::Error] or returning an
    /// event.
    fn handle_cmd(&self, id: &E::Id, state: &E) -> Result<E::Evt, Self::Error>;

    /// The reply function, which is applied if the command handler has returned an event (as
    /// opposed to a rejection) and after that has been persisted successfully.
    fn reply(self, state: &E) -> Self::Reply;
}

/// A handle representing a spawned [EventSourced] entity, which can be used to pass it commands
/// which have been registerd before via [EventSourcedEntity::cmd].
#[derive(Debug, Clone)]
pub struct EntityRef<E, T>
where
    E: EventSourced,
{
    cmd_in: mpsc::Sender<(BoxedAny, oneshot::Sender<Result<BoxedAny, BoxedAny>>)>,
    id: E::Id,
    _e: PhantomData<E>,
    _t: PhantomData<T>,
}

impl<E, T> EntityRef<E, T>
where
    E: EventSourced + 'static,
{
    /// The ID of the represented [EventSourced] entity.
    pub fn id(&self) -> &E::Id {
        &self.id
    }

    /// Pass the given command to the represented [EventSourced] entity. The returned value is a
    /// nested result where the outer one represents technical errors, e.g. problems connecting to
    /// the event log, and the inner one comes from the command handler, i.e. signals potential
    /// command rejection.
    #[instrument(skip(self))]
    pub async fn handle_cmd<C, X>(
        &self,
        cmd: C,
    ) -> Result<Result<C::Reply, C::Error>, HandleCmdError>
    where
        C: Cmd<E> + Debug + Send,
        T: CoproductSelector<C, X>,
    {
        let (result_in, result_out) = oneshot::channel();
        self.cmd_in
            .send((Box::new(cmd), result_in))
            .await
            .map_err(|_| HandleCmdError("cannot send cmd".to_string()))?;
        let result = result_out
            .await
            .map_err(|_| HandleCmdError("cannot receive cmd handler result".to_string()))?;
        let result = result
            .map_err(|error| *error.downcast::<C::Error>().expect("downcast error"))
            .map(|reply| *reply.downcast::<C::Reply>().expect("downcast reply"));
        Ok(result)
    }
}

/// Extension methods for [EventSourced] entities.
pub trait EventSourcedExt
where
    Self: EventSourced + Sized,
{
    /// Create a new [EventSourced] entity with the given type name, ID and this [EventSourced]
    /// implementation.
    fn entity(self, type_name: &'static str, id: Self::Id) -> EventSourcedEntity<Self, CNil> {
        EventSourcedEntity {
            e: self,
            type_name,
            id,
            cmd_fns: HashMap::new(),
            _t: PhantomData,
        }
    }
}

impl<E> EventSourcedExt for E where E: EventSourced {}

/// An [EventSourced] entity which allows for registering `Cmd`s and `spawn`ing.
pub struct EventSourcedEntity<E, T>
where
    E: EventSourced,
{
    e: E,
    type_name: &'static str,
    id: E::Id,
    cmd_fns: BoxedCmdFns<E>,
    _t: PhantomData<T>,
}

impl<E, T> EventSourcedEntity<E, T>
where
    E: EventSourced + Debug + Send + Sync + 'static,
    E::Evt: Debug + Send + Sync + 'static,
    E::Id: Debug + Clone + Send,
    T: Send + 'static,
{
    /// Add the given command to the [EventSourced] entity.
    pub fn cmd<C>(mut self) -> EventSourcedEntity<E, Coprod!(C, ...T)>
    where
        C: Cmd<E>,
    {
        let type_id = TypeId::of::<C>();
        let cmd_handler = boxed_handle_cmd::<C, E>();
        let reply_handler = boxed_reply::<C, E>();
        self.cmd_fns.insert(type_id, (cmd_handler, reply_handler));

        EventSourcedEntity {
            e: self.e,
            type_name: self.type_name,
            id: self.id,
            cmd_fns: self.cmd_fns,
            _t: PhantomData,
        }
    }

    /// Spawn this [EventSourced] entity with the given settings, event log, snapshot store and
    /// `Binarize` functions.
    ///
    /// The resulting type will look like the following example, where `Counter` is the
    /// `EventSourced` implementation and `Increase` and `Decrease` have been added as `Cmd`s in
    /// that order:
    ///
    /// `EntityRef<Counter, Coprod!(Decrease, Increase)>`
    #[instrument(skip(self, evt_log, snapshot_store, binarize))]
    pub async fn spawn<L, M, B>(
        self,
        snapshot_after: Option<NonZeroU64>,
        cmd_buffer: NonZeroUsize,
        mut evt_log: L,
        mut snapshot_store: M,
        binarize: B,
    ) -> Result<EntityRef<E, T>, SpawnError>
    where
        L: EvtLog<Id = E::Id>,
        M: SnapshotStore<Id = E::Id>,
        B: Binarize<E::Evt, E>,
    {
        // Restore snapshot.
        let (snapshot_seq_no, state) = snapshot_store
            .load::<E, _, _>(&self.id, |bytes| binarize.state_from_bytes(bytes))
            .await
            .map_err(|error| SpawnError::LoadSnapshot(error.into()))?
            .map(|Snapshot { seq_no, state }| {
                debug!(id = ?self.id, seq_no, ?state, "restored snapshot");
                (seq_no, state)
            })
            .unzip();
        let mut state = state.unwrap_or(self.e);

        // Get and validate last sequence number.
        let mut last_seq_no = evt_log
            .last_seq_no(self.type_name, &self.id)
            .await
            .map_err(|error| SpawnError::LastNonZeroU64(error.into()))?;
        if last_seq_no < snapshot_seq_no {
            return Err(SpawnError::InvalidLastSeqNo(last_seq_no, snapshot_seq_no));
        };

        // Replay latest events.
        if snapshot_seq_no < last_seq_no {
            let seq_no = snapshot_seq_no
                .map(|n| n.saturating_add(1))
                .unwrap_or(NonZeroU64::MIN);
            let to_seq_no = last_seq_no.unwrap(); // This is safe because of the above relation!
            debug!(id = ?self.id, seq_no, to_seq_no, "replaying evts");

            let evts = evt_log
                .evts_by_id::<E::Evt, _, _>(self.type_name, &self.id, seq_no, move |bytes| {
                    binarize.evt_from_bytes(bytes)
                })
                .await
                .map_err(|error| SpawnError::EvtsById(error.into()))?;

            state = evts
                .map_err(|error| SpawnError::NextEvt(error.into()))
                .take_until_predicate(move |result| {
                    result
                        .as_ref()
                        .ok()
                        .map(|&(seq_no, _)| seq_no >= to_seq_no)
                        .unwrap_or(true)
                })
                .try_fold(state, |state, (_, evt)| ok(state.handle_evt(evt)))
                .await?;

            debug!(id = ?self.id, state = ?state, "replayed evts");
        }

        // Spawn handler loop.
        let (cmd_in, mut cmd_out) = mpsc::channel::<(
            BoxedAny,
            oneshot::Sender<Result<BoxedAny, BoxedAny>>,
        )>(cmd_buffer.get());
        let id = self.id.clone();
        task::spawn({
            let mut evt_count = 0u64;

            async move {
                while let Some((cmd, result_sender)) = cmd_out.recv().await {
                    debug!(id = ?self.id, "handling cmd");

                    let type_id = cmd.as_ref().type_id();
                    let (handle_cmd_fn, reply_fn) =
                        self.cmd_fns.get(&type_id).expect("get cmd handler");
                    let result = handle_cmd_fn(&cmd, &self.id, &state);
                    match result {
                        Ok(evt) => {
                            debug!(id = ?self.id, ?evt, "persisting event");

                            match evt_log
                                .persist::<E::Evt, _, _>(
                                    self.type_name,
                                    &self.id,
                                    last_seq_no,
                                    &evt,
                                    &|evt| binarize.evt_to_bytes(evt),
                                )
                                .await
                            {
                                Ok(seq_no) => {
                                    debug!(id = ?self.id, ?evt, seq_no, "persited event");

                                    last_seq_no = Some(seq_no);
                                    state = state.handle_evt(evt);

                                    evt_count += 1;
                                    if snapshot_after
                                        .map(|a| evt_count % a == 0)
                                        .unwrap_or_default()
                                    {
                                        debug!(id = ?self.id, seq_no, evt_count, "saving snapshot");

                                        if let Err(error) = snapshot_store
                                            .save(&self.id, seq_no, &state, &|state| {
                                                binarize.state_to_bytes(state)
                                            })
                                            .await
                                        {
                                            error!(
                                                error = error.as_chain(),
                                                id = ?self.id,
                                                "cannot save snapshot"
                                            );
                                        };
                                    }

                                    let reply = reply_fn(cmd, &state);
                                    if result_sender.send(Ok(reply)).is_err() {
                                        error!(id = ?self.id, "cannot send cmd reply");
                                    };
                                }

                                Err(error) => {
                                    error!(error = error.as_chain(), id = ?self.id, "cannot persist event");
                                    // This is fatal, we must terminate the entity!
                                    break;
                                }
                            }
                        }

                        Err(error) => {
                            if result_sender.send(Err(error)).is_err() {
                                error!(id = ?self.id, "cannot send cmd error");
                            }
                        }
                    };
                }

                debug!(id = ?self.id, "entity terminated");
            }
        });

        Ok(EntityRef {
            cmd_in,
            id,
            _e: PhantomData,
            _t: PhantomData,
        })
    }
}

/// A technical error, signaling that a command cannot be sent from an [EntityRef] to its event
/// sourced entity or the result cannot be received from its event sourced entity.
#[derive(Debug, Error, Serialize, Deserialize)]
#[error("{0}")]
pub struct HandleCmdError(String);

/// A technical error when spawning an [EventSourced] entity.
#[derive(Debug, Error)]
pub enum SpawnError {
    #[error("cannot load snapshot from snapshot store")]
    LoadSnapshot(#[source] BoxError),

    #[error("last sequence number {0:?} less than snapshot sequence number {0:?}")]
    InvalidLastSeqNo(Option<NonZeroU64>, Option<NonZeroU64>),

    #[error("cannot get last seqence number from event log")]
    LastNonZeroU64(#[source] BoxError),

    #[error("cannot get events by ID stream from event log")]
    EvtsById(#[source] BoxError),

    #[error("cannot get next event from events by ID stream")]
    NextEvt(#[source] BoxError),
}

fn boxed_handle_cmd<C, E>() -> BoxedHandleCmd<E>
where
    C: Cmd<E>,
    E: EventSourced,
{
    Box::new(move |cmd, id, e| {
        let cmd = cmd.downcast_ref::<C>().expect("downcast cmd");
        cmd.handle_cmd(id, e).map_err(|error| {
            let error: BoxedAny = Box::new(error);
            error
        })
    })
}

fn boxed_reply<C, E>() -> BoxedReply<E>
where
    C: Cmd<E>,
    E: EventSourced,
{
    Box::new(move |cmd, e| {
        let cmd = *cmd.downcast::<C>().expect("downcast cmd");
        Box::new(cmd.reply(e))
    })
}

#[cfg(all(test, feature = "serde_json"))]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use bytes::Bytes;
    use futures::{stream, Stream, StreamExt};
    use std::{error::Error as StdError, iter};
    use tracing_test::traced_test;
    use uuid::Uuid;

    #[derive(Debug, Clone)]
    struct TestEvtLog;

    impl EvtLog for TestEvtLog {
        type Id = Uuid;
        type Error = TestEvtLogError;

        async fn persist<E, ToBytes, ToBytesError>(
            &mut self,
            _type_name: &'static str,
            _id: &Self::Id,
            last_seq_no: Option<NonZeroU64>,
            _evt: &E,
            _to_bytes: &ToBytes,
        ) -> Result<NonZeroU64, Self::Error>
        where
            E: Sync,
            ToBytes: Fn(&E) -> Result<Bytes, ToBytesError> + Sync,
            ToBytesError: StdError + Send + Sync + 'static,
        {
            let seq_no = last_seq_no.unwrap_or(NonZeroU64::MIN);
            Ok(seq_no)
        }

        async fn last_seq_no(
            &self,
            _type_name: &'static str,
            _id: &Self::Id,
        ) -> Result<Option<NonZeroU64>, Self::Error> {
            Ok(Some(42.try_into().unwrap()))
        }

        async fn evts_by_id<E, FromBytes, FromBytesError>(
            &self,
            _type_name: &'static str,
            id: &Self::Id,
            from_seq_no: NonZeroU64,
            from_bytes: FromBytes,
        ) -> Result<impl Stream<Item = Result<(NonZeroU64, E), Self::Error>> + Send, Self::Error>
        where
            E: Send,
            FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Copy + Send + Sync,
            FromBytesError: StdError + Send + Sync + 'static,
        {
            let successors = iter::successors(Some(from_seq_no), |n| n.checked_add(1));
            let evts = stream::iter(successors).map(move |n| {
                let evt = from_bytes(
                    serde_json::to_vec(&Evt::Increased(id.to_owned(), 1))
                        .unwrap()
                        .into(),
                )
                .unwrap();
                Ok((n, evt))
            });

            Ok(evts)
        }

        async fn evts_by_type<E, FromBytes, FromBytesError>(
            &self,
            _type_name: &'static str,
            _seq_no: NonZeroU64,
            _from_bytes: FromBytes,
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
    struct TestEvtLogError(#[source] BoxError);

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
            let bytes = serde_json::to_vec(&21).unwrap();
            let state = state_from_bytes(bytes.into()).unwrap();
            Ok(Some(Snapshot {
                seq_no: 21.try_into().unwrap(),
                state,
            }))
        }
    }

    #[derive(Debug, Error)]
    #[error("TestSnapshotStoreError")]
    struct TestSnapshotStoreError;

    // EventSourced ===============================================================================

    #[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
    pub struct Counter(u64);

    impl EventSourced for Counter {
        type Id = Uuid;
        type Evt = Evt;

        fn handle_evt(self, evt: Evt) -> Self {
            match evt {
                Evt::Increased(_, n) => Self(self.0 + n),
                Evt::Decreased(_, n) => Self(self.0 - n),
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub enum Evt {
        Increased(Uuid, u64),
        Decreased(Uuid, u64),
    }

    #[derive(Debug)]
    pub struct Increase(pub u64);

    impl Cmd<Counter> for Increase {
        type Error = Overflow;
        type Reply = u64;

        fn handle_cmd(&self, id: &Uuid, state: &Counter) -> Result<Evt, Self::Error> {
            if u64::MAX - state.0 < self.0 {
                Err(Overflow)
            } else {
                Ok(Evt::Increased(*id, self.0))
            }
        }

        fn reply(self, state: &Counter) -> Self::Reply {
            state.0
        }
    }

    #[derive(Debug)]
    pub struct Overflow;

    #[derive(Debug)]
    pub struct Decrease(pub u64);

    impl Cmd<Counter> for Decrease {
        type Error = Underflow;
        type Reply = u64;

        fn handle_cmd(&self, id: &Uuid, state: &Counter) -> Result<Evt, Self::Error> {
            if state.0 < self.0 {
                Err(Underflow)
            } else {
                Ok(Evt::Decreased(*id, self.0))
            }
        }

        fn reply(self, state: &Counter) -> Self::Reply {
            state.0
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    pub struct Underflow;

    #[tokio::test]
    #[traced_test]
    async fn test() -> Result<(), BoxError> {
        let evt_log = TestEvtLog;
        let snapshot_store = TestSnapshotStore;

        let entity: EntityRef<Counter, Coprod!(Decrease, Increase)> = Counter::default()
            .entity("counter", Uuid::from_u128(1))
            .cmd::<Increase>()
            .cmd::<Decrease>()
            .spawn(
                None,
                unsafe { NonZeroUsize::new_unchecked(1) },
                evt_log,
                snapshot_store,
                binarize::serde_json::SerdeJsonBinarize,
            )
            .await?;

        let reply = entity.handle_cmd(Increase(1)).await?;
        assert_matches!(reply, Ok(43));
        let reply = entity.handle_cmd(Decrease(100)).await?;
        assert_matches!(reply, Err(error) if error == Underflow);
        let reply = entity.handle_cmd(Decrease(1)).await?;
        assert_matches!(reply, Ok(42));

        assert!(logs_contain("state=Counter(42)"));

        Ok(())
    }
}
