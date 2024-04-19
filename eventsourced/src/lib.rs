#![cfg_attr(docsrs, feature(doc_cfg))]

//! Event sourced entities.
//!
//! EventSourced is inspired to a large degree by the amazing
//! [Akka Persistence](https://doc.akka.io/docs/akka/current/typed/index-persistence.html) library.
//! It provides a framework for implementing
//! [Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html) and
//! [CQRS](https://www.martinfowler.com/bliki/CQRS.html).
//!
//! The [EventSourced] trait defines the event type and handling for event sourced entities. These
//! are identifiable by a type name and ID and can be created with the [EventSourcedExt::entity]
//! extension method. Commands can be defined via the [Command] trait which contains a command
//! handler function to either reject a command or return an event. An event gets persisted to the
//! event log and then applied to the event handler to return the new state of the entity.
//!
//! ```text
//!                  ┌───────┐   ┌ ─ ─ ─ Entity─ ─ ─ ─
//!                  │Command│                        │
//! ┌ ─ ─ ─ ─ ─ ─    └───────┘   │ ┌────────────────┐
//!     Client   │────────────────▶│ handle_command │─┼─────────┐
//! └ ─ ─ ─ ─ ─ ─                │ └────────────────┘           │
//!        ▲                           │    │         │         │ ┌─────┐
//!         ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─│─ ─ ─     │read               │ │Event│
//!                   ┌─────┐               ▼         │         ▼ └─────┘
//!                   │Reply│    │     ┌─────────┐       ┌ ─ ─ ─ ─ ─ ─
//!                   │  /  │          │  State  │    │     EventLog  │
//!                   │Error│    │     └─────────┘       └ ─ ─ ─ ─ ─ ─
//!                   └─────┘               ▲         │         │ ┌─────┐
//!                              │     write│                   │ │Event│
//!                                         │         │         │ └─────┘
//!                              │ ┌────────────────┐           │
//!                                │  handle_event  │◀┼─────────┘
//!                              │ └────────────────┘
//!                                                   │
//!                              └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//! ```
//!
//! The [EventLog] and [SnapshotStore] traits define a pluggable event log and a pluggable snapshot
//! store respectively. For [NATS](https://nats.io/) and [Postgres](https://www.postgresql.org/)
//! these are implemented in the respective crates.
//!
//! [EventSourcedEntity::spawn] puts the event sourced entity on the given event log and snapshot
//! store, returning an [EntityRef] which can be cheaply cloned and used to pass commands to the
//! entity. Conversion of events and snapshot state to and from bytes happens via the given
//! [Binarize] implementation; for [prost](https://github.com/tokio-rs/prost) and [serde_json](https://github.com/serde-rs/json)
//! these are already provided. Snapshots are taken after the configured number of processed events
//! to speed up future spawning.
//!
//! [EntityRef::handle_command] either returns [Command::Error] for a rejected command or
//! [Command::Reply] for an accepted one, wrapped in another `Result` dealing with technical errors.
//!
//! Events can be queried from the event log by ID or by entity type. These queries can be used to
//! build read side projections. There is early support for projections in the
//! `eventsourced-projection` crate.

pub mod binarize;
pub mod event_log;
pub mod snapshot_store;

mod util;

use crate::{
    binarize::Binarize,
    event_log::EventLog,
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

type BoxedCommand<E> = Box<dyn ErasedCommand<E> + Send>;
type BoxedCommandEffect<E> = Result<
    (
        <E as EventSourced>::Event,
        Box<dyn FnOnce(&E) -> BoxedAny + Send + Sync>,
    ),
    BoxedAny,
>;
type BoxedAny = Box<dyn Any + Send>;
type BoxedMsg<E> = (BoxedCommand<E>, oneshot::Sender<Result<BoxedAny, BoxedAny>>);

/// State and event handling for an [EventSourcedEntity].
pub trait EventSourced
where
    Self: Send + Sync + Sized + 'static,
{
    /// The Id type.
    type Id: Debug + Clone + Send;

    /// The event type.
    type Event: Debug + Send + Sync + 'static;

    /// The type name.
    const TYPE_NAME: &'static str;

    /// The event handler.
    fn handle_event(self, event: Self::Event) -> Self;
}

/// A command for a [EventSourced] implementation, defining command handling and replying.
pub trait Command<E>
where
    Self: Debug + Send + 'static,
    E: EventSourced,
{
    /// The type for replies.
    type Reply: Send + 'static;

    /// The type for rejecting this command.
    type Error: Send + 'static;

    /// The command handler, taking this command, and references to the ID and the state of
    /// the event sourced entity, either rejecting this command via [CommandEffect::reject] or
    /// returning an event using [CommandEffect::emit_and_reply] (or [CommandEffect::emit] in
    /// case `Reply = ()`).
    fn handle_command(self, id: &E::Id, state: &E) -> CommandEffect<E, Self::Reply, Self::Error>;
}

/// The result of handling a command, either emitting an event and replying or rejecting the
/// command.
pub enum CommandEffect<E, Reply, Error>
where
    E: EventSourced,
{
    EmitAndReply(E::Event, Box<dyn FnOnce(&E) -> Reply + Send + Sync>),
    Reject(Error),
}

impl<E, Reply, Error> CommandEffect<E, Reply, Error>
where
    E: EventSourced,
{
    /// Emit the given event, persist it, and after applying it to the state, use the given function
    /// to create a reply. The new state is passed to the function after applying the event.
    pub fn emit_and_reply(
        event: E::Event,
        make_reply: impl FnOnce(&E) -> Reply + Send + Sync + 'static,
    ) -> Self {
        Self::EmitAndReply(event, Box::new(make_reply))
    }

    /// Reject this command with the given error.
    pub fn reject(error: Error) -> Self {
        Self::Reject(error)
    }
}

impl<E, Error> CommandEffect<E, (), Error>
where
    E: EventSourced,
{
    /// Persist the given event (and don't give a reply for Commands with Reply = ()).
    pub fn emit(event: E::Event) -> Self {
        Self::emit_and_reply(event, |_| ())
    }
}

/// A handle representing a spawned [EventSourcedEntity], which can be used to pass it commands.
#[derive(Debug, Clone)]
pub struct EntityRef<E>
where
    E: EventSourced,
{
    command_in: mpsc::Sender<BoxedMsg<E>>,
    id: E::Id,
    _e: PhantomData<E>,
}

impl<E> EntityRef<E>
where
    E: EventSourced,
{
    /// The ID of the represented [EventSourcedEntity].
    pub fn id(&self) -> &E::Id {
        &self.id
    }

    /// Pass the given command to the represented [EventSourcedEntity]. The returned value is a
    /// nested result where the outer one represents technical errors, e.g. problems connecting to
    /// the event log, and the inner one comes from the command handler, i.e. signals potential
    /// command rejection.
    #[instrument(skip(self))]
    pub async fn handle_command<C>(
        &self,
        command: C,
    ) -> Result<Result<C::Reply, C::Error>, HandleCommandError>
    where
        C: Command<E>,
    {
        let (result_in, result_out) = oneshot::channel();
        self.command_in
            .send((Box::new(command), result_in))
            .await
            .map_err(|_| HandleCommandError("cannot send command".to_string()))?;
        let result = result_out
            .await
            .map_err(|_| HandleCommandError("cannot receive command handler result".to_string()))?;
        let result = result
            .map_err(|error| *error.downcast::<C::Error>().expect("downcast error"))
            .map(|reply| *reply.downcast::<C::Reply>().expect("downcast reply"));
        Ok(result)
    }
}

/// Extension methods for [EventSourced] entities.
pub trait EventSourcedExt
where
    Self: EventSourced,
{
    /// Create a new [EventSourcedEntity] for this [EventSourced] implementation.
    fn entity(self) -> EventSourcedEntity<Self> {
        EventSourcedEntity(self)
    }
}

impl<E> EventSourcedExt for E where E: EventSourced {}

/// An [EventSourcedEntity] which can be `spawn`ed.
#[derive(Debug, Clone)]
pub struct EventSourcedEntity<E>(E)
where
    E: EventSourced;

impl<E> EventSourcedEntity<E>
where
    E: EventSourced,
{
    /// Spawn this [EventSourcedEntity] with the given ID, settings, event log, snapshot store and
    /// `Binarize` functions.
    #[instrument(skip(self, event_log, snapshot_store, binarize))]
    pub async fn spawn<L, S, B>(
        self,
        id: E::Id,
        snapshot_after: Option<NonZeroU64>,
        command_buffer: NonZeroUsize,
        mut event_log: L,
        mut snapshot_store: S,
        binarize: B,
    ) -> Result<EntityRef<E>, SpawnError>
    where
        L: EventLog<Id = E::Id>,
        S: SnapshotStore<Id = E::Id>,
        B: Binarize<E::Event, E>,
    {
        // Restore snapshot.
        let (snapshot_seq_no, state) = snapshot_store
            .load::<E, _, _>(&id, |bytes| binarize.state_from_bytes(bytes))
            .await
            .map_err(|error| SpawnError::LoadSnapshot(error.into()))?
            .map(|Snapshot { seq_no, state }| {
                debug!(?id, seq_no, "restored snapshot");
                (seq_no, state)
            })
            .unzip();
        let mut state = state.unwrap_or(self.0);

        // Get and validate last sequence number.
        let mut last_seq_no = event_log
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
            debug!(?id, seq_no, to_seq_no, "replaying events");

            let events = event_log
                .events_by_id::<E::Event, _, _>(E::TYPE_NAME, &id, seq_no, move |bytes| {
                    binarize.event_from_bytes(bytes)
                })
                .await
                .map_err(|error| SpawnError::EventsById(error.into()))?;

            state = events
                .map_err(|error| SpawnError::NextEvent(error.into()))
                .take_until_predicate(move |result| {
                    result
                        .as_ref()
                        .ok()
                        .map(|&(seq_no, _)| seq_no >= to_seq_no)
                        .unwrap_or(true)
                })
                .try_fold(state, |state, (_, event)| ok(state.handle_event(event)))
                .await?;

            debug!(?id, "replayed events");
        }

        // Spawn handler loop.
        let (command_in, mut command_out) = mpsc::channel::<BoxedMsg<E>>(command_buffer.get());
        task::spawn({
            let id = id.clone();
            let mut event_count = 0u64;

            async move {
                while let Some((command, result_sender)) = command_out.recv().await {
                    debug!(?id, ?command, "handling command");

                    let result = command.handle_command(&id, &state);
                    match result {
                        Ok((event, make_reply)) => {
                            debug!(?id, ?event, "persisting event");

                            match event_log
                                .persist::<E::Event, _, _>(
                                    E::TYPE_NAME,
                                    &id,
                                    last_seq_no,
                                    &event,
                                    &|event| binarize.event_to_bytes(event),
                                )
                                .await
                            {
                                Ok(seq_no) => {
                                    debug!(?id, ?event, seq_no, "persited event");

                                    last_seq_no = Some(seq_no);
                                    state = state.handle_event(event);

                                    event_count += 1;
                                    if snapshot_after
                                        .map(|a| event_count % a == 0)
                                        .unwrap_or_default()
                                    {
                                        debug!(?id, seq_no, event_count, "saving snapshot");

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

                                    let reply = make_reply(&state);
                                    if result_sender.send(Ok(reply)).is_err() {
                                        error!(?id, "cannot send command reply");
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
                                error!(?id, "cannot send command error");
                            }
                        }
                    };
                }

                debug!(?id, "entity terminated");
            }
        });

        Ok(EntityRef {
            command_in,
            id,
            _e: PhantomData,
        })
    }
}

/// A technical error, signaling that a command cannot be sent from an [EntityRef] to its event
/// sourced entity or the result cannot be received from its event sourced entity.
#[derive(Debug, Error, Serialize, Deserialize)]
#[error("{0}")]
pub struct HandleCommandError(String);

/// A technical error when spawning an [EventSourcedEntity].
#[derive(Debug, Error)]
pub enum SpawnError {
    #[error("cannot load snapshot from snapshot store")]
    LoadSnapshot(#[source] BoxError),

    #[error("last sequence number {0:?} less than snapshot sequence number {0:?}")]
    InvalidLastSeqNo(Option<NonZeroU64>, Option<NonZeroU64>),

    #[error("cannot get last seqence number from event log")]
    LastNonZeroU64(#[source] BoxError),

    #[error("cannot get events by ID stream from event log")]
    EventsById(#[source] BoxError),

    #[error("cannot get next event from events by ID stream")]
    NextEvent(#[source] BoxError),
}

trait ErasedCommand<E>
where
    Self: Debug,
    E: EventSourced,
{
    fn handle_command(self: Box<Self>, id: &E::Id, state: &E) -> BoxedCommandEffect<E>;
}

impl<C, E, Reply, Error> ErasedCommand<E> for C
where
    C: Command<E, Reply = Reply, Error = Error>,
    E: EventSourced,
    Reply: Send + 'static,
    Error: Send + 'static,
{
    fn handle_command(self: Box<Self>, id: &E::Id, state: &E) -> BoxedCommandEffect<E> {
        match <C as Command<E>>::handle_command(*self, id, state) {
            CommandEffect::EmitAndReply(event, make_reply) => {
                Ok((event, Box::new(|s| Box::new(make_reply(s)))))
            }
            CommandEffect::Reject(error) => Err(Box::new(error) as BoxedAny),
        }
    }
}

#[cfg(all(test, feature = "serde_json"))]
mod tests {
    use crate::{
        binarize::serde_json::*,
        event_log::{test::TestEventLog, EventLog},
        snapshot_store::{test::TestSnapshotStore, SnapshotStore},
        Command, CommandEffect, EntityRef, EventSourced, EventSourcedExt,
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
        type Event = CounterEvent;

        const TYPE_NAME: &'static str = "counter";

        fn handle_event(self, event: CounterEvent) -> Self {
            match event {
                CounterEvent::Increased(_, n) => Self(self.0 + n),
                CounterEvent::Decreased(_, n) => Self(self.0 - n),
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub enum CounterEvent {
        Increased(Uuid, u64),
        Decreased(Uuid, u64),
    }

    #[derive(Debug)]
    pub struct IncreaseCounter(pub u64);

    impl Command<Counter> for IncreaseCounter {
        type Error = Overflow;
        type Reply = u64;

        fn handle_command(
            self,
            id: &Uuid,
            state: &Counter,
        ) -> CommandEffect<Counter, u64, Overflow> {
            if u64::MAX - state.0 < self.0 {
                CommandEffect::reject(Overflow)
            } else {
                CommandEffect::emit_and_reply(
                    CounterEvent::Increased(*id, self.0),
                    |state: &Counter| state.0,
                )
            }
        }
    }

    #[derive(Debug)]
    pub struct Overflow;

    #[derive(Debug)]
    pub struct DecreaseCounter(pub u64);

    impl Command<Counter> for DecreaseCounter {
        type Error = Underflow;
        type Reply = u64;

        fn handle_command(
            self,
            id: &Uuid,
            state: &Counter,
        ) -> CommandEffect<Counter, u64, Underflow> {
            if state.0 < self.0 {
                CommandEffect::reject(Underflow)
            } else {
                CommandEffect::emit_and_reply(
                    CounterEvent::Decreased(*id, self.0),
                    move |state: &Counter| state.0 + self.0 - self.0, /* Simple no-op test to
                                                                       * verify that closing
                                                                       * over this command is
                                                                       * possible */
                )
            }
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    pub struct Underflow;

    #[tokio::test]
    #[traced_test]
    async fn test() -> Result<(), BoxError> {
        let id = Uuid::from_u128(1);

        let mut event_log = TestEventLog::default();
        for _ in 0..42 {
            event_log
                .persist(
                    "counter",
                    &id,
                    None,
                    &CounterEvent::Increased(id, 1),
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
                event_log,
                snapshot_store,
                SerdeJsonBinarize,
            )
            .await?;

        let reply = entity.handle_command(IncreaseCounter(1)).await?;
        assert_matches!(reply, Ok(43));
        let reply = entity.handle_command(DecreaseCounter(100)).await?;
        assert_matches!(reply, Err(error) if error == Underflow);
        let reply = entity.handle_command(DecreaseCounter(1)).await?;
        assert_matches!(reply, Ok(42));

        Ok(())
    }
}
