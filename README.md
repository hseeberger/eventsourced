# EventSourced

Event sourced entities in [Rust](https://www.rust-lang.org/).

## Crates

- `eventsourced`: core library with `EventSourced`, `Entity`, `EvtLog`, `SnapshotStore`, etc.
- `eventsourced-nats`: [NATS](https://nats.io/) implementation for eventsourced `EvtLog` and `SnapshotStore`
- `eventsourced-postgres`: [Postgres](https://www.postgresql.org/) implementation for eventsourced`EvtLog` and `SnapshotStore`

## Concepts

EventSourced is inspired to a large degree by the excellent [Akka Persistence](https://doc.akka.io/docs/akka/current/typed/index-persistence.html) library.

The `EventSourced` trait defines types for commands, events, snapshot state and errors as well as functions for command handling, event handling and setting a snapshot state.

The `EvtLog` and `SnapshotStore` traits define a pluggable event log and a pluggable snapshot store respectively. For [Postgres](https://www.postgresql.org/) and [NATS](https://nats.io/) these are already provided.

The `Entity` struct and its associated `spawn` fuction provide for creating "running" instances of an `EventSourced` implementation, identifiable by a `Uuid`, for some event log and some snapshot store. Conversion of events and snapshot state to and from bytes happens via functions; for [prost](https://github.com/tokio-rs/prost) and [serde_json](https://github.com/serde-rs/json) these are already provided.

## Counter example (no pun intended)

The `counter` package in the `example` directory contains a simple example: a counter which handles `Inc` and `Decrease` commands and emits/handles `Increased` and `Decreased` events.

```rust
impl EventSourced for Counter {
    ...

    fn handle_cmd(&self, cmd: Self::Cmd) -> Result<Vec<Self::Evt>, Self::Error> {
        match cmd {
            Cmd::Inc(inc) => {
                if inc > u64::MAX - self.value {
                    Err(Error::Overflow {
                        value: self.value,
                        inc,
                    })
                } else {
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

There are also the two `counter-nats` and `counter-postgres` packages, with a biary crate each, using `eventsourced-nats` and `eventsourced-postgres` respectively for the event log.

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

Take a look at the `example` directory for more details.


## License ##

This code is open source software licensed under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).
