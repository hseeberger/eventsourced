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
//! extension method. Commands can be defined via the [Cmd] trait which contains a command handler
//! function to either reject a command or return an event. An event gets persisted to the event log
//! and then applied to the event handler to return the new state of the entity.
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
pub mod evt_log;
pub mod snapshot_store;

mod util;

use crate::{
    binarize::Binarize,
    evt_log::EvtLog,
    snapshot_store::{Snapshot, SnapshotStore},
    util::StreamExt as ThisStreamExt,
};
use error_ext::{BoxError, StdErrorExt};
use futures::{future::ok, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::{
    any::Any,
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

type BoxedCmd<E> = Box<dyn ErasedCmd<E> + Send>;
type BoxedAny = Box<dyn Any + Send>;
type BoxedMsg<E> = (BoxedCmd<E>, oneshot::Sender<Result<BoxedAny, BoxedAny>>);

/// The state of an event sourced entity as well as its event handling (which transforms the state).
pub trait EventSourced {
    /// The Id type.
    type Id: Debug + Clone + Send;

    /// The event type.
    type Evt: Debug + Send + Sync + 'static;

    /// The type name.
    const TYPE_NAME: &'static str;

    /// The event handler.
    fn handle_evt(self, evt: &Self::Evt) -> Self;
}

/// A command for the given [EventSourced] implementation, defining its handling and replying.
pub trait Cmd<E>
where
    Self: Debug + Send + 'static,
    E: EventSourced,
{
    /// The type for replies.
    type Reply: Send + 'static;

    /// The type for rejecting this command.
    type Error: Send + 'static;

    /// The command handler, taking this command, and references to the ID and the state of
    /// the event sourced entity, either rejecting this command via [Self::Error] or returning an
    /// event.
    fn handle_cmd(&self, id: &E::Id, state: &E) -> Result<E::Evt, Self::Error>;

    /// The reply factory, which is applied if the command handler has returned an event (as
    /// opposed to a rejection) and after that has been persisted successfully.
    fn make_reply(&self, id: &E::Id, state: &E, evt: E::Evt) -> Self::Reply;
}

/// A handle representing a spawned [EventSourced] entity, which can be used to pass it commands.
#[derive(Debug, Clone)]
pub struct EntityRef<E>
where
    E: EventSourced,
{
    cmd_in: mpsc::Sender<BoxedMsg<E>>,
    id: E::Id,
    _e: PhantomData<E>,
}

impl<E> EntityRef<E>
where
    E: EventSourced,
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
    pub async fn handle_cmd<C>(&self, cmd: C) -> Result<Result<C::Reply, C::Error>, HandleCmdError>
    where
        C: Cmd<E>,
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
    fn entity(self) -> EventSourcedEntity<Self> {
        EventSourcedEntity(self)
    }
}

impl<E> EventSourcedExt for E where E: EventSourced {}

/// An [EventSourced] entity which allows for registering `Cmd`s and `spawn`ing.
#[derive(Debug, Clone)]
pub struct EventSourcedEntity<E>(E)
where
    E: EventSourced;

impl<E> EventSourcedEntity<E>
where
    E: EventSourced + Debug + Send + Sync + 'static,
{
    /// Spawn this [EventSourced] entity with the given settings, event log, snapshot store and
    /// `Binarize` functions.
    ///
    /// The resulting type will look like the following example, where `Counter` is the
    /// `EventSourced` implementation and `Increase` and `Decrease` have been added as `Cmd`s in
    /// that order:
    ///
    /// `EntityRef<Counter, Coprod!(Decrease, Increase)>`
    #[instrument(skip(self, evt_log, snapshot_store, binarize))]
    pub async fn spawn<L, S, B>(
        self,
        id: E::Id,
        snapshot_after: Option<NonZeroU64>,
        cmd_buffer: NonZeroUsize,
        mut evt_log: L,
        mut snapshot_store: S,
        binarize: B,
    ) -> Result<EntityRef<E>, SpawnError>
    where
        L: EvtLog<Id = E::Id>,
        S: SnapshotStore<Id = E::Id>,
        B: Binarize<E::Evt, E>,
    {
        // Restore snapshot.
        let (snapshot_seq_no, state) = snapshot_store
            .load::<E, _, _>(&id, |bytes| binarize.state_from_bytes(bytes))
            .await
            .map_err(|error| SpawnError::LoadSnapshot(error.into()))?
            .map(|Snapshot { seq_no, state }| {
                debug!(?id, seq_no, ?state, "restored snapshot");
                (seq_no, state)
            })
            .unzip();
        let mut state = state.unwrap_or(self.0);

        // Get and validate last sequence number.
        let mut last_seq_no = evt_log
            .last_seq_no(E::TYPE_NAME, &id)
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
            debug!(?id, seq_no, to_seq_no, "replaying evts");

            let evts = evt_log
                .evts_by_id::<E::Evt, _, _>(E::TYPE_NAME, &id, seq_no, move |bytes| {
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
                .try_fold(state, |state, (_, evt)| ok(state.handle_evt(&evt)))
                .await?;

            debug!(?id, state = ?state, "replayed evts");
        }

        // Spawn handler loop.
        let (cmd_in, mut cmd_out) = mpsc::channel::<BoxedMsg<E>>(cmd_buffer.get());
        task::spawn({
            let id = id.clone();
            let mut evt_count = 0u64;

            async move {
                while let Some((cmd, result_sender)) = cmd_out.recv().await {
                    debug!(?id, ?cmd, "handling cmd");

                    let result = cmd.handle_cmd(&id, &state);
                    match result {
                        Ok(evt) => {
                            debug!(?id, ?evt, "persisting event");

                            match evt_log
                                .persist::<E::Evt, _, _>(
                                    E::TYPE_NAME,
                                    &id,
                                    last_seq_no,
                                    &evt,
                                    &|evt| binarize.evt_to_bytes(evt),
                                )
                                .await
                            {
                                Ok(seq_no) => {
                                    debug!(?id, ?evt, seq_no, "persited event");

                                    last_seq_no = Some(seq_no);
                                    state = state.handle_evt(&evt);

                                    evt_count += 1;
                                    if snapshot_after
                                        .map(|a| evt_count % a == 0)
                                        .unwrap_or_default()
                                    {
                                        debug!(?id, seq_no, evt_count, "saving snapshot");

                                        if let Err(error) = snapshot_store
                                            .save(&id, seq_no, &state, &|state| {
                                                binarize.state_to_bytes(state)
                                            })
                                            .await
                                        {
                                            error!(
                                                error = error.as_chain(),
                                                ?id,
                                                "cannot save snapshot"
                                            );
                                        };
                                    }

                                    let reply = cmd.make_reply(&id, &state, evt);
                                    if result_sender.send(Ok(reply)).is_err() {
                                        error!(?id, "cannot send cmd reply");
                                    };
                                }

                                Err(error) => {
                                    error!(error = error.as_chain(), ?id, "cannot persist event");
                                    // This is fatal, we must terminate the entity!
                                    break;
                                }
                            }
                        }

                        Err(error) => {
                            if result_sender.send(Err(error)).is_err() {
                                error!(?id, "cannot send cmd error");
                            }
                        }
                    };
                }

                debug!(?id, "entity terminated");
            }
        });

        Ok(EntityRef {
            cmd_in,
            id,
            _e: PhantomData,
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

trait ErasedCmd<E>
where
    Self: Debug,
    E: EventSourced,
{
    fn handle_cmd(&self, id: &E::Id, state: &E) -> Result<E::Evt, BoxedAny>;

    fn make_reply(&self, id: &E::Id, state: &E, evt: E::Evt) -> BoxedAny;
}

impl<C, E, Reply, Error> ErasedCmd<E> for C
where
    C: Cmd<E, Reply = Reply, Error = Error>,
    E: EventSourced,
    Reply: Send + 'static,
    Error: Send + 'static,
{
    fn handle_cmd(&self, id: &E::Id, state: &E) -> Result<E::Evt, BoxedAny> {
        let result = self.handle_cmd(id, state);
        result.map_err(|error| Box::new(error) as BoxedAny)
    }

    fn make_reply(&self, id: &E::Id, state: &E, evt: E::Evt) -> BoxedAny {
        Box::new(self.make_reply(id, state, evt))
    }
}

#[cfg(all(test, feature = "serde_json"))]
mod tests {
    use crate::{
        binarize::serde_json::*,
        evt_log::{test::TestEvtLog, EvtLog},
        snapshot_store::{test::TestSnapshotStore, SnapshotStore},
        Cmd, EntityRef, EventSourced, EventSourcedExt,
    };
    use assert_matches::assert_matches;
    use error_ext::BoxError;
    use serde::{Deserialize, Serialize};
    use tracing_test::traced_test;
    use uuid::Uuid;

    #[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
    pub struct Counter(u64);

    impl EventSourced for Counter {
        type Id = Uuid;
        type Evt = CounterEvt;

        const TYPE_NAME: &'static str = "counter";

        fn handle_evt(self, evt: &CounterEvt) -> Self {
            match evt {
                CounterEvt::Increased(_, n) => Self(self.0 + n),
                CounterEvt::Decreased(_, n) => Self(self.0 - n),
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub enum CounterEvt {
        Increased(Uuid, u64),
        Decreased(Uuid, u64),
    }

    #[derive(Debug)]
    pub struct IncreaseCounter(pub u64);

    impl Cmd<Counter> for IncreaseCounter {
        type Error = Overflow;
        type Reply = u64;

        fn handle_cmd(&self, id: &Uuid, state: &Counter) -> Result<CounterEvt, Self::Error> {
            if u64::MAX - state.0 < self.0 {
                Err(Overflow)
            } else {
                Ok(CounterEvt::Increased(*id, self.0))
            }
        }

        fn make_reply(&self, _id: &Uuid, state: &Counter, _evt: CounterEvt) -> Self::Reply {
            state.0
        }
    }

    #[derive(Debug)]
    pub struct Overflow;

    #[derive(Debug)]
    pub struct DecreaseCounter(pub u64);

    impl Cmd<Counter> for DecreaseCounter {
        type Error = Underflow;
        type Reply = u64;

        fn handle_cmd(&self, id: &Uuid, state: &Counter) -> Result<CounterEvt, Self::Error> {
            if state.0 < self.0 {
                Err(Underflow)
            } else {
                Ok(CounterEvt::Decreased(*id, self.0))
            }
        }

        fn make_reply(&self, _id: &Uuid, state: &Counter, _evt: CounterEvt) -> Self::Reply {
            state.0
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    pub struct Underflow;

    #[tokio::test]
    #[traced_test]
    async fn test() -> Result<(), BoxError> {
        let id = Uuid::from_u128(1);

        let mut evt_log = TestEvtLog::default();
        for _ in 0..42 {
            evt_log
                .persist(
                    "counter",
                    &id,
                    None,
                    &CounterEvt::Increased(id, 1),
                    &to_bytes,
                )
                .await?;
        }

        let mut snapshot_store = TestSnapshotStore::default();
        snapshot_store
            .save(&id, 21.try_into()?, &Counter(21), &to_bytes)
            .await?;

        let entity: EntityRef<Counter> = Counter::default()
            .entity()
            .spawn(
                id,
                None,
                1.try_into()?,
                evt_log,
                snapshot_store,
                SerdeJsonBinarize,
            )
            .await?;

        assert!(logs_contain("state=Counter(42)"));

        let reply = entity.handle_cmd(IncreaseCounter(1)).await?;
        assert_matches!(reply, Ok(43));
        let reply = entity.handle_cmd(DecreaseCounter(100)).await?;
        assert_matches!(reply, Err(error) if error == Underflow);
        let reply = entity.handle_cmd(DecreaseCounter(1)).await?;
        assert_matches!(reply, Ok(42));

        Ok(())
    }
}
