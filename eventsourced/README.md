# EventSourced

[![Crates.io][crates-badge]][crates-url]
[![license][license-badge]][license-url]

[crates-badge]: https://img.shields.io/crates/v/eventsourced
[crates-url]: https://crates.io/crates/eventsourced
[license-badge]: https://img.shields.io/github/license/hseeberger/eventsourced
[license-url]: https://github.com/hseeberger/eventsourced/blob/main/LICENSE

Event sourced entities in [Rust](https://www.rust-lang.org/).

## Concepts

EventSourced is inspired to a large degree by the amazing
[Akka Persistence](https://doc.akka.io/docs/akka/current/typed/index-persistence.html) library.
It provides a framework for implementing
[Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html) and
[CQRS](https://www.martinfowler.com/bliki/CQRS.html).

The [EventSourced] trait defines the event type and handling for event sourced entities. These
are identifiable by a type name and ID and can be created with the [EventSourcedExt::entity]
extension method. Commands can be defined via the [Command] trait which contains a command handler
function to either reject a command or return an event. An event gets persisted to the event log
and then applied to the event handler to return the new state of the entity.

```text
                 ┌───────┐   ┌ ─ ─ ─ Entity─ ─ ─ ─
                 │Command│                        │
┌ ─ ─ ─ ─ ─ ─    └───────┘   │ ┌────────────────┐
    Client   │────────────────▶│ handle_command │─┼─────────┐
└ ─ ─ ─ ─ ─ ─                │ └────────────────┘           │
       ▲                           │    │         │         │ ┌─────┐
        ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─│─ ─ ─     │read               │ │Event│
                  ┌─────┐               ▼         │         ▼ └─────┘
                  │Reply│    │     ┌─────────┐       ┌ ─ ─ ─ ─ ─ ─
                  │  /  │          │  State  │    │     EventLog  │
                  │Error│    │     └─────────┘       └ ─ ─ ─ ─ ─ ─
                  └─────┘               ▲         │         │ ┌─────┐
                             │     write│                   │ │Event│
                                        │         │         │ └─────┘
                             │ ┌────────────────┐           │
                               │  handle_event  │◀┼─────────┘
                             │ └────────────────┘
                                                  │
                             └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
```

The [EventLog] and [SnapshotStore] traits define a pluggable event log and a pluggable snapshot
store respectively. For [Postgres](https://www.postgresql.org/) these are implemented in the respective crates.

[EventSourcedEntity::spawn] puts the event sourced entity on the given event log and snapshot
store, returning an [EntityRef] which can be cheaply cloned and used to pass commands to the
entity. Conversion of events and snapshot state to and from bytes happens via the given
[Binarize] implementation; for [prost](https://github.com/tokio-rs/prost) and [serde_json](https://github.com/serde-rs/json)
these are already provided. Snapshots are taken after the configured number of processed events
to speed up future spawning.

[EntityRef::handle_command] either returns [Command::Error] for a rejected command or [Command::Reply] for
an accepted one, wrapped in another `Result` dealing with technical errors.

Events can be queried from the event log by ID or by entity type. These queries can be used to
build read side projections. There is early support for projections in the
`eventsourced-projection` crate.

## Requirements for building the project and examples

Before building the project and examples, please make sure you have installed the [protobuf](https://github.com/protocolbuffers/protobuf) dependency, if using the optional byte conversion with prost.

On macOS `protobuf` can be installed via Homebrew:

```
brew install protobuf
```

## Counter example (no pun intended)

The `counter` package in the `example` directory contains a simple example: a counter which handles `Inc` and `Dec` commands and emits/handles `Increased` and `Decreased` events.

```rust
#[derive(Debug)]
struct Increase(u64);

impl Command<Uuid, Event, Counter> for Increase {
    type Error = Overflow;

    type Reply = u64;

    fn handle(self, id: &Uuid, state: &Counter) -> Result<Event, Self::Error> {
        if u64::MAX - state.0 < self.0 {
            Err(Overflow)
        } else {
            Ok(Event::Increased(*id))
        }
    }

    fn reply(state: &Counter) -> Self::Reply {
        state.0
    }
}

#[derive(Debug)]
struct Overflow;

#[derive(Debug)]
struct Decrease(u64);

impl Command<Uuid, Event, Counter> for Decrease {
    type Error = Underflow;

    type Reply = Counter;

    fn handle(self, id: &Uuid, state: &Counter) -> Result<Event, Self::Error> {
        if state.0 < self.0 {
            Err(Underflow)
        } else {
            Ok(Event::Decreased(*id))
        }
    }

    fn reply(state: &Counter) -> Self::Reply {
        *state
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Underflow;

#[derive(Debug, Serialize, Deserialize)]
enum Event {
    Increased(Uuid),
    Decreased(Uuid),
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
struct Counter(u64);

impl Counter {
    fn handle_event(self, evt: Event) -> Self {
        match evt {
            Event::Increased(_) => Self(self.0 + 1),
            Event::Decreased(_) => Self(self.0 - 1),
        }
    }
}
```

There is also the `counter-postgres` packages, with a binary crate, using `eventsourced-postgres` as event log.

```rust
...
let evt_log = evt_log.clone();
let snapshot_store = snapshot_store.clone();
let counter = Counter::default();
let counter = counter
    .spawn(
        id,
        unsafe { NonZeroUsize::new_unchecked(42) },
        evt_log,
        snapshot_store,
        convert::serde_json::binarizer(),
    )
    .await
    .context("cannot spawn entity")?;

tasks.spawn(async move {
    for n in 0..config.evt_count / 2 {
        if n > 0 && n % 2_500 == 0 {
            println!("{id}: {} events persisted", n * 2);
        }
        counter
            .handle_command(Command::Inc(n as u64))
            .await
            .context("cannot handle Inc command")
            .unwrap();
        ...
    }
});
...
```

Take a look at the [examples](../examples) directory for more details.

### Running the counter-postgres example

For the `counter-postgres` example, PostgreSQL needs to be installed. On macOS just use Homebrew:

```
brew install postgresql@14
```

Before running the example, start PostgreSQL:

```
brew services run postgresql@14
```

Make sure you know the following connection parameters:
- host
- port
- user
- password
- dbname

Change the configuration either by changing the `default.toml` file at `examples/counter-postgres/config` or by overriding the configuration settings with environment variables, e.g. `APP__EVT_LOG__DBNAME=test` or `APP__COUNTER__EVT_COUNT=42`:

Then use the following command to run the example:

```
RUST_LOG=info \
    APP__EVT_LOG__DBNAME=test \
    APP__COUNTER__EVT_COUNT=42 \
    CONFIG_DIR=examples/counter-postgres/config \
    cargo run \
    --release \
    --package counter-postgres
```

## License ##

This code is open source software licensed under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).
