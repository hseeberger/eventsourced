# EventSourced

[![Crates.io][crates-badge]][crates-url]
[![license][license-badge]][license-url]
[![build][build-badge]][build-url]

[crates-badge]: https://img.shields.io/crates/v/eventsourced
[crates-url]: https://crates.io/crates/eventsourced
[license-badge]: https://img.shields.io/github/license/hseeberger/eventsourced
[license-url]: https://github.com/hseeberger/eventsourced/blob/main/LICENSE
[build-badge]: https://img.shields.io/github/actions/workflow/status/hseeberger/eventsourced/ci.yaml
[build-url]: https://github.com/hseeberger/eventsourced/actions/workflows/ci.yaml

Event sourced entities in [Rust](https://www.rust-lang.org/).

## Crates

- `eventsourced`: core library with `EventSourced`, `Entity`, `EvtLog`, `SnapshotStore`, etc.
- `eventsourced-nats`: [NATS](https://nats.io/) implementation for `EvtLog` and `SnapshotStore`
- `eventsourced-postgres`: [Postgres](https://www.postgresql.org/) implementation for `EvtLog` and `SnapshotStore`

## Concepts

EventSourced is inspired to a large degree by the amazing [Akka Persistence](https://doc.akka.io/docs/akka/current/typed/index-persistence.html) library.

The `EventSourced` trait defines types for commands, events, snapshot state and errors as well as methods for command handling, event handling and setting a snapshot state.

The `EvtLog` and `SnapshotStore` traits define a pluggable event log and a pluggable snapshot store respectively. For [NATS](https://nats.io/) and [Postgres](https://www.postgresql.org/) these are implemented in the respective crates.

The `spawn` extension method provides for creating "running" instances of an `EventSourced` implementation, identifiable by a `Uuid`, for some event log and some snapshot store. Conversion of events and snapshot state to and from bytes happens via given "binarizer" functions; for [prost](https://github.com/tokio-rs/prost) and [serde_json](https://github.com/serde-rs/json) these are already provided.

## Requirements for building the project

Before building the project and examples, please make sure you have installed the [protobuf](https://github.com/protocolbuffers/protobuf) dependency that is not only needed for the optional byte conversion with prost, which is a default feature, but also for eventsourced-nats. The only way to get away without `protobuf` is to change the default features and not build eventsourced-nats.

On macOS `protobuf` can be installed via Homebrew:

```
brew install protobuf
```

## Counter example (no pun intended)

The `counter` package in the `example` directory contains a simple example: a counter which handles `Inc` and `Dec` commands and emits/handles `Increased` and `Decreased` events.

```rust
impl EventSourced for Counter {
    ...

    /// Command handler, returning the to be persisted events or an error.
    fn handle_cmd(&self, cmd: Self::Cmd) -> Result<Vec<Self::Evt>, Self::Error> {
        match cmd {
            Cmd::Inc(inc) => {
                // Validate command: overflow.
                if inc + self.value > u64::MAX {
                    Err(Error::Overflow {
                        value: self.value,
                        inc,
                    })
                }
                // Valid Inc command results in Increased event.
                else {
                    Ok(vec![Evt {
                        evt: Some(evt::Evt::Increased(Increased {
                            old_value: self.value,
                            inc,
                        })),
                    }])
                }
            }
            ...
        }
    }

    /// Event handler, returning whether to take a snapshot or not.
    fn handle_evt(&mut self, seq_no: u64, evt: &Self::Evt) -> Option<Self::State> {
        match evt.evt {
            Some(evt::Evt::Increased(Increased { old_value, inc })) => {
                self.value += inc;
                debug!(seq_no, old_value, inc, value = self.value, "Increased");
            }
            ...
        }

        self.snapshot_after.and_then(|snapshot_after| {
            if seq_no % snapshot_after == 0 {
                Some(self.value)
            } else {
                None
            }
        })
    }
    ...
}
```

There are also the two `counter-nats` and `counter-postgres` packages, with a binary crate each, using `eventsourced-nats` and `eventsourced-postgres` respectively for the event log.

```rust
...
let evt_log = evt_log.clone();
let snapshot_store = snapshot_store.clone();
let counter = Counter::default().with_snapshot_after(config.snapshot_after);
let counter = Entity::spawn(
    id,
    counter,
    42,
    evt_log,
    snapshot_store,
    convert::prost::binarizer(),
)
.await
.context("Cannot spawn entity")?;

tasks.spawn(async move {
    for n in 0..config.evt_count / 2 {
        if n > 0 && n % 2_500 == 0 {
            println!("{id}: {} events persisted", n * 2);
        }
        let _ = counter
            .handle_cmd(Cmd::Inc(n as u64))
            .await
            .context("Cannot handle Inc command")
            .unwrap()
            .context("Invalid command")
            .unwrap();
        let _ = counter
            .handle_cmd(Cmd::Decrease(n as u64))
            .await
            .context("Cannot handle Dec command")
            .unwrap()
            .context("Invalid command")
            .unwrap();
    }
});
...
```

Take a look at the [examples](examples) directory for more details.

### Running the counter-nats example

For the `counter-nats` example, nats-server needs to be installed. On macOS just use Homebrew:

```
brew install nats-server
```

Before running the example, start the nats-server with the `jetstream` feature enabled:

```
nats-server --jetstream
```

Then use the following command to run the example:

```
RUST_LOG=info \
    CONFIG_DIR=examples/counter-postgres/config \
    cargo run \
    --release \
    --package counter-nats
```

Notice that you can change the configuration either by changing the `defaul.yaml` file at `examples/counter-nats/config` or by overriding the configuration settings with environment variables, e.g. `APP__COUNTER__EVT_COUNT=42`:

```
RUST_LOG=info \
    APP__COUNTER__EVT_COUNT=42 \
    CONFIG_DIR=examples/counter-nats/config \
    cargo run \
    --release \
    --package counter-nats
```

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

Change the configuration either by changing the `defaul.yaml` file at `examples/counter-postgres/config` or by overriding the configuration settings with environment variables, e.g. `APP__EVT_LOG__DBNAME=test` or `APP__COUNTER__EVT_COUNT=42`:

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
