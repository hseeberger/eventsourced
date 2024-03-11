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
//! The [Entity] struct represents event sourced entities, identifiable by a type name and ID. To
//! create one, an event handler function and one or more commands – implementations of the `Cmd`
//! trait – have to be given. `Cmd` defines a command handler function to either reject a command or
//! return an event. An event gets persisted to the event log and then applied to the event handler
//! to return the new state of the entity.
//!
//! [Entity::spawn] puts the entity on the given event log and snapshot store, returning an
//! [EntityRef] which can be cheaply cloned and used to pass commands to the entity. Conversion of
//! events and snapshot state to and from bytes happens via the given [Binarize] implementation; for
//! [prost](https://github.com/tokio-rs/prost) and [serde_json](https://github.com/serde-rs/json)
//! these are already provided. Snapshots are taken after the configured number of processed events
//! to speed up future spawning.
//!
//! [EntityRef::handle_cmd] either returns `Cmd::Error` for a rejected command or `Cmd::Reply` for
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
use frunk::{coproduct::CoproductSelector, Coprod};
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
type BoxedCmdHandler<I, E, S> = Box<dyn Fn(BoxedAny, &I, &S) -> Result<E, BoxedAny> + Send>;
type BoxedMakeReply<S> = Box<dyn Fn(&S) -> BoxedAny + Send + Sync>;
type CmdHandlerMakeReply<I, E, S> = HashMap<TypeId, (BoxedCmdHandler<I, E, S>, BoxedMakeReply<S>)>;

/// An event sourced entity.
pub struct Entity<I, T, E, S> {
    type_name: &'static str,
    id: I,
    cmd_handler_make_reply: CmdHandlerMakeReply<I, E, S>,
    _t: PhantomData<T>,
    _e: PhantomData<E>,
    _s: PhantomData<S>,
}

impl<I, E, S> Entity<I, (), E, S> {
    /// Create a new event sourced entity with the given type name, ID and event handler.
    pub fn new(type_name: &'static str, id: I) -> Entity<I, Coprod!(), E, S>
    where
        I: 'static,
        E: 'static,
        S: 'static,
    {
        Entity {
            type_name,
            id,
            cmd_handler_make_reply: HashMap::new(),
            _t: PhantomData,
            _e: PhantomData,
            _s: PhantomData,
        }
    }
}

impl<I, T, E, S> Entity<I, T, E, S>
where
    I: Debug + Clone + Send + 'static,
    E: Debug + Send + Sync + 'static,
    S: State<E> + Debug + Default + Send + Sync + 'static,
    T: Send + 'static,
{
    /// Add another command `C` to the event sourced entity.
    pub fn add_cmd<C>(mut self) -> Entity<I, Coprod!(C, ...T), E, S>
    where
        C: Cmd<I, E, S>,
    {
        let type_id = TypeId::of::<C>();
        let cmd_handler = boxed_cmd_handler::<I, C, E, S>();
        let reply_handler = boxed_reply_handler::<I, C, E, S>();
        self.cmd_handler_make_reply
            .insert(type_id, (cmd_handler, reply_handler));

        Entity {
            id: self.id,
            type_name: self.type_name,
            cmd_handler_make_reply: self.cmd_handler_make_reply,
            _t: PhantomData,
            _e: PhantomData,
            _s: PhantomData,
        }
    }

    /// Spawn this event sourced entity with the given settings, event log, snapshot store and
    /// `Binarize` functions.
    ///
    /// The resulting type will look like the following example, where `Uuid` is used as ID type,
    /// `Increase` and `Decrease` are registered as `Cmd`s in that order, `Evt` is the event type
    /// and `Counter` is the state type:
    ///
    /// `EntityRef<Uuid, Coprod!(Decrease, Increase), Evt, Counter>`
    #[instrument(skip(self, evt_log, snapshot_store, binarize))]
    pub async fn spawn<L, M, B>(
        self,
        snapshot_after: Option<NonZeroU64>,
        cmd_buffer: NonZeroUsize,
        mut evt_log: L,
        mut snapshot_store: M,
        binarize: B,
    ) -> Result<EntityRef<I, T, E, S>, SpawnError>
    where
        L: EvtLog<Id = I>,
        M: SnapshotStore<Id = I>,
        B: Binarize<E, S>,
    {
        // Restore snapshot.
        let (snapshot_seq_no, state) = snapshot_store
            .load::<S, _, _>(&self.id, |bytes| binarize.state_from_bytes(bytes))
            .await
            .map_err(|error| SpawnError::LoadSnapshot(error.into()))?
            .map(|Snapshot { seq_no, state }| {
                debug!(id = ?self.id, seq_no, ?state, "restored snapshot");
                (seq_no, state)
            })
            .unzip();
        let mut state = state.unwrap_or_default();

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
                .evts_by_id::<E, _, _>(self.type_name, &self.id, seq_no, move |bytes| {
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
                    let (cmd_handler, reply_handler) = self
                        .cmd_handler_make_reply
                        .get(&type_id)
                        .expect("get cmd handler");
                    let result = cmd_handler(cmd, &self.id, &state);
                    match result {
                        Ok(evt) => {
                            debug!(id = ?self.id, ?evt, "persisting event");

                            match evt_log
                                .persist::<E, _, _>(
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

                                    let reply = reply_handler(&state);
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
            _t: PhantomData,
            _e: PhantomData,
            _s: PhantomData,
        })
    }
}

/// Error for spawning an event sourced entity.
#[derive(Debug, Error)]
pub enum SpawnError {
    #[error("cannot load snapshot from snapshot store")]
    LoadSnapshot(#[source] BoxError),

    #[error("last sequence number {0:?} less than snapshot sequence number {0:?}")]
    InvalidLastSeqNo(Option<NonZeroU64>, Option<NonZeroU64>),

    #[error("cannot get last seqence number from event log")]
    LastNonZeroU64(#[source] BoxError),

    #[error("cannot get events by ID from event log")]
    EvtsById(#[source] BoxError),

    #[error("cannot get next event from event log")]
    NextEvt(#[source] BoxError),
}

/// A command and its handling for an [Entity].
pub trait Cmd<I, E, S>
where
    Self: 'static,
{
    /// The type for rejecting this command.
    type Error: Send + 'static;

    /// The type returned by the [reply](Cmd::reply) function.
    type Reply: Send + 'static;

    /// The command handler function, taking this command, and references to the ID and the state of
    /// the [Entity], either rejecting this command via [Self::Error] or returning an event.
    fn handle(self, id: &I, state: &S) -> Result<E, Self::Error>;

    /// The reply function.
    fn reply(state: &S) -> Self::Reply;
}

/// State and event handling for an [Entity].
pub trait State<E> {
    /// Event handler.
    fn handle_evt(self, evt: E) -> Self;
}

/// A handle representing a spawned [Entity], which can be used to pass it commands implementing
/// [Cmd].
#[derive(Debug, Clone)]
pub struct EntityRef<I, T, E, S> {
    cmd_in: mpsc::Sender<(BoxedAny, oneshot::Sender<Result<BoxedAny, BoxedAny>>)>,
    id: I,
    _t: PhantomData<T>,
    _e: PhantomData<E>,
    _s: PhantomData<S>,
}

impl<I, T, E, S> EntityRef<I, T, E, S>
where
    E: 'static,
{
    /// The ID of the represented [Entity].
    pub fn id(&self) -> &I {
        &self.id
    }

    /// Pass the given command to the represented [Entity].
    #[instrument(skip(self))]
    pub async fn handle_cmd<C, X>(
        &self,
        cmd: C,
    ) -> Result<Result<C::Reply, C::Error>, HandleCmdError>
    where
        C: Cmd<I, E, S> + Debug + Send,
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

/// A command cannot be sent from an [EntityRef] to its [Entity] or the result cannot be received
/// from its [Entity].
#[derive(Debug, Error, Serialize, Deserialize)]
#[error("{0}")]
pub struct HandleCmdError(String);

fn boxed_cmd_handler<I, C, E, S>() -> BoxedCmdHandler<I, E, S>
where
    C: Cmd<I, E, S>,
{
    Box::new(move |cmd, id, state| {
        let cmd = *cmd.downcast::<C>().expect("downcast cmd");
        C::handle(cmd, id, state).map_err(|error| {
            let error: BoxedAny = Box::new(error);
            error
        })
    })
}

fn boxed_reply_handler<I, C, E, S>() -> Box<dyn Fn(&S) -> BoxedAny + Send + Sync>
where
    C: Cmd<I, E, S>,
    C::Reply: 'static,
{
    Box::new(move |state| Box::new(C::reply(state)))
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

    include!("../../examples/counter/src/counter.in");

    #[tokio::test]
    #[traced_test]
    async fn test() -> Result<(), BoxError> {
        let evt_log = TestEvtLog;
        let snapshot_store = TestSnapshotStore;

        let entity: EntityRef<Uuid, Coprod!(Decrease, Increase), Evt, Counter> =
            Entity::new("counter", Uuid::from_u128(1))
                .add_cmd::<Increase>()
                .add_cmd::<Decrease>()
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
