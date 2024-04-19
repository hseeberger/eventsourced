# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.5.2...eventsourced-projection-v0.6.0) - 2024-04-19

### Other
- rename cmd command and evt event ([#232](https://github.com/hseeberger/eventsourced/pull/232))

## [0.5.2](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.5.1...eventsourced-projection-v0.5.2) - 2024-04-10

### Other
- updated the following local packages: eventsourced

## [0.5.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.5.0...eventsourced-projection-v0.5.1) - 2024-03-27

### Other
- update Cargo.toml dependencies

## [0.5.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.4.1...eventsourced-projection-v0.5.0) - 2024-03-20

### Other
- simplify API and get rid of type level programming ([#205](https://github.com/hseeberger/eventsourced/pull/205))
- use trait-variant rewrite

## [0.4.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.4.0...eventsourced-projection-v0.4.1) - 2024-03-17

### Other
- updated the following local packages: eventsourced

## [0.4.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.3.6...eventsourced-projection-v0.4.0) - 2024-03-15

### Other
- rewrite api ([#192](https://github.com/hseeberger/eventsourced/pull/192))

## [0.3.6](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.3.5...eventsourced-projection-v0.3.6) - 2024-03-11

### Other
- updated the following local packages: eventsourced

## [0.3.5](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.3.4...eventsourced-projection-v0.3.5) - 2024-02-19

### Other
- updated the following local packages: eventsourced

## [0.3.4](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.3.3...eventsourced-projection-v0.3.4) - 2024-02-17

### Other
- major cleanup ([#177](https://github.com/hseeberger/eventsourced/pull/177))
- some cleanup, activate step in GH workflows again ([#175](https://github.com/hseeberger/eventsourced/pull/175))
- update deps ([#173](https://github.com/hseeberger/eventsourced/pull/173))
- use error-ext crate ([#169](https://github.com/hseeberger/eventsourced/pull/169))

## [0.3.3](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.3.2...eventsourced-projection-v0.3.3) - 2024-02-02

### Other
- updated the following local packages: eventsourced

## [0.3.2](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.3.1...eventsourced-projection-v0.3.2) - 2024-02-01

### Other
- error_chain no longer pub ([#164](https://github.com/hseeberger/eventsourced/pull/164))

## [0.3.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.3.0...eventsourced-projection-v0.3.1) - 2024-01-26

### Fixed
- projection run adds one to stored seq_no ([#159](https://github.com/hseeberger/eventsourced/pull/159))

## [0.3.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.2.1...eventsourced-projection-v0.3.0) - 2024-01-26

### Other
- rename LoadError to Error

## [0.2.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.2.0...eventsourced-projection-v0.2.1) - 2024-01-26

### Fixed
- projection run respects stored seq_no ([#156](https://github.com/hseeberger/eventsourced/pull/156))

## [0.2.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.1.0...eventsourced-projection-v0.2.0) - 2024-01-23

### Other
- rename postgres

## [0.0.1](https://github.com/hseeberger/eventsourced/releases/tag/eventsourced-projection-v0.0.1) - 2024-01-16

### Added
- add projections ([#148](https://github.com/hseeberger/eventsourced/pull/148))

## [0.10.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.9.0...eventsourced-nats-v0.10.0) - 2023-12-20

### Added
- add const TYPE_NAME to trait EventSourced ([#94](https://github.com/hseeberger/eventsourced/pull/94))

## [0.9.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.8.5...eventsourced-nats-v0.9.0) - 2023-12-08

### Added
- introduce entity type ([#90](https://github.com/hseeberger/eventsourced/pull/90))

## [0.8.5](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.8.4...eventsourced-nats-v0.8.5) - 2023-11-30

### Other
- updated the following local packages: eventsourced, eventsourced

## [0.8.4](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.8.3...eventsourced-nats-v0.8.4) - 2023-11-29

### Other
- use ToString instead of Into<String> ([#84](https://github.com/hseeberger/eventsourced/pull/84))

## [0.8.3](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.8.2...eventsourced-nats-v0.8.3) - 2023-11-29

### Other
- updated the following local packages: eventsourced, eventsourced

## [0.8.2](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.8.1...eventsourced-nats-v0.8.2) - 2023-11-24

### Fixed
- *(eventsourced-postgres)* sequence number handling ([#76](https://github.com/hseeberger/eventsourced/pull/76))

## [0.7.2](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.7.1...eventsourced-nats-v0.7.2) - 2023-11-05

### Other
- more cleanup ([#69](https://github.com/hseeberger/eventsourced/pull/69))
- minor code style cleanup ([#68](https://github.com/hseeberger/eventsourced/pull/68))
- separate readme for each crate ([#66](https://github.com/hseeberger/eventsourced/pull/66))
