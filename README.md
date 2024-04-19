# EventSourced

[![license][license-badge]][license-url]
[![build][build-badge]][build-url]

[license-badge]: https://img.shields.io/github/license/hseeberger/eventsourced
[license-url]: https://github.com/hseeberger/eventsourced/blob/main/LICENSE
[build-badge]: https://img.shields.io/github/actions/workflow/status/hseeberger/eventsourced/ci.yaml
[build-url]: https://github.com/hseeberger/eventsourced/actions/workflows/ci.yaml

Event sourced entities in [Rust](https://www.rust-lang.org/).

## Crates

- [`eventsourced`](https://github.com/hseeberger/eventsourced/blob/main/eventsourced/README.md): core library with `EventSourced`, `Entity`, `EventLog`, `SnapshotStore`, etc.
- [`eventsourced-nats`](https://github.com/hseeberger/eventsourced/blob/main/eventsourced-nats/README.md): [NATS](https://nats.io/) implementation for `EventLog` and `SnapshotStore`
- [`eventsourced-postgres`](https://github.com/hseeberger/eventsourced/blob/main/eventsourced-postgres/README.md): [Postgres](https://www.postgresql.org/) implementation for `EventLog` and `SnapshotStore`
- [`eventsourced-projection`](https://github.com/hseeberger/eventsourced/blob/main/eventsourced-projection/README.md): early support for the CQRS read side

## License ##

This code is open source software licensed under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).
