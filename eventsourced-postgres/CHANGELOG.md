# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.14.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-postgres-v0.13.16...eventsourced-postgres-v0.14.0) - 2024-04-19

### Other
- rename cmd command and evt event ([#232](https://github.com/hseeberger/eventsourced/pull/232))

## [0.13.16](https://github.com/hseeberger/eventsourced/compare/eventsourced-postgres-v0.13.15...eventsourced-postgres-v0.13.16) - 2024-04-10

### Other
- updated the following local packages: eventsourced, eventsourced

## [0.13.15](https://github.com/hseeberger/eventsourced/compare/eventsourced-postgres-v0.13.14...eventsourced-postgres-v0.13.15) - 2024-03-27

### Other
- update Cargo.toml dependencies

## [0.13.14](https://github.com/hseeberger/eventsourced/compare/eventsourced-postgres-v0.13.13...eventsourced-postgres-v0.13.14) - 2024-03-20

### Other
- simplify API and get rid of type level programming ([#205](https://github.com/hseeberger/eventsourced/pull/205))

## [0.13.13](https://github.com/hseeberger/eventsourced/compare/eventsourced-postgres-v0.13.12...eventsourced-postgres-v0.13.13) - 2024-03-17

### Other
- updated the following local packages: eventsourced, eventsourced

## [0.13.12](https://github.com/hseeberger/eventsourced/compare/eventsourced-postgres-v0.13.11...eventsourced-postgres-v0.13.12) - 2024-03-15

### Other
- rewrite api ([#192](https://github.com/hseeberger/eventsourced/pull/192))

## [0.13.11](https://github.com/hseeberger/eventsourced/compare/eventsourced-postgres-v0.13.10...eventsourced-postgres-v0.13.11) - 2024-03-11

### Other
- updated the following local packages: eventsourced, eventsourced

## [0.13.10](https://github.com/hseeberger/eventsourced/compare/eventsourced-postgres-v0.13.9...eventsourced-postgres-v0.13.10) - 2024-02-19

### Other
- updated the following local packages: eventsourced, eventsourced

## [0.13.9](https://github.com/hseeberger/eventsourced/compare/eventsourced-postgres-v0.13.8...eventsourced-postgres-v0.13.9) - 2024-02-17

### Other
- major cleanup ([#177](https://github.com/hseeberger/eventsourced/pull/177))

## [0.13.8](https://github.com/hseeberger/eventsourced/compare/eventsourced-postgres-v0.13.7...eventsourced-postgres-v0.13.8) - 2024-02-02

### Other
- updated the following local packages: eventsourced, eventsourced

## [0.13.7](https://github.com/hseeberger/eventsourced/compare/eventsourced-postgres-v0.13.6...eventsourced-postgres-v0.13.7) - 2024-02-01

### Other
- updated the following local packages: eventsourced, eventsourced

## [0.13.6](https://github.com/hseeberger/eventsourced/compare/eventsourced-postgres-v0.13.5...eventsourced-postgres-v0.13.6) - 2024-01-26

### Other
- updated the following local packages: eventsourced, eventsourced

## [0.13.5](https://github.com/hseeberger/eventsourced/compare/eventsourced-postgres-v0.13.4...eventsourced-postgres-v0.13.5) - 2024-01-23

### Other
- update Cargo.toml dependencies

## [0.13.4](https://github.com/hseeberger/eventsourced/compare/eventsourced-postgres-v0.13.3...eventsourced-postgres-v0.13.4) - 2024-01-16

### Added
- add projections ([#148](https://github.com/hseeberger/eventsourced/pull/148))

### Other
- clearer EvtLog API ([#147](https://github.com/hseeberger/eventsourced/pull/147))

## [0.13.3](https://github.com/hseeberger/eventsourced/compare/eventsourced-postgres-v0.13.2...eventsourced-postgres-v0.13.3) - 2024-01-12

### Other
- updated the following local packages: eventsourced, eventsourced

## [0.13.2](https://github.com/hseeberger/eventsourced/compare/eventsourced-postgres-v0.13.1...eventsourced-postgres-v0.13.2) - 2024-01-06

### Other
- updated the following local packages: eventsourced, eventsourced

## [0.13.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-postgres-v0.13.0...eventsourced-postgres-v0.13.1) - 2024-01-04

### Other
- minor renaming
- cleaner sequence number handling ([#130](https://github.com/hseeberger/eventsourced/pull/130))

## [0.12.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-postgres-v0.11.0...eventsourced-postgres-v0.12.0) - 2023-12-28

### Added
- no longer support tagging ([#116](https://github.com/hseeberger/eventsourced/pull/116))
- add logging and tracing ([#114](https://github.com/hseeberger/eventsourced/pull/114))
- use NonZeroU64 instead of SeqNo ([#111](https://github.com/hseeberger/eventsourced/pull/111))

### Fixed
- hydrate entities / evtlog after seq_no ([#120](https://github.com/hseeberger/eventsourced/pull/120))

## [0.10.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-postgres-v0.9.0...eventsourced-postgres-v0.10.0) - 2023-12-20

### Added
- add const TYPE_NAME to trait EventSourced ([#94](https://github.com/hseeberger/eventsourced/pull/94))

## [0.9.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-postgres-v0.8.5...eventsourced-postgres-v0.9.0) - 2023-12-08

### Added
- introduce entity type ([#90](https://github.com/hseeberger/eventsourced/pull/90))

## [0.8.5](https://github.com/hseeberger/eventsourced/compare/eventsourced-postgres-v0.8.4...eventsourced-postgres-v0.8.5) - 2023-11-30

### Other
- updated the following local packages: eventsourced, eventsourced

## [0.8.4](https://github.com/hseeberger/eventsourced/compare/eventsourced-postgres-v0.8.3...eventsourced-postgres-v0.8.4) - 2023-11-29

### Other
- use ToString instead of Into<String> ([#84](https://github.com/hseeberger/eventsourced/pull/84))

## [0.8.3](https://github.com/hseeberger/eventsourced/compare/eventsourced-postgres-v0.8.2...eventsourced-postgres-v0.8.3) - 2023-11-29

### Other
- updated the following local packages: eventsourced, eventsourced

## [0.8.2](https://github.com/hseeberger/eventsourced/compare/eventsourced-postgres-v0.8.1...eventsourced-postgres-v0.8.2) - 2023-11-24

### Fixed
- *(eventsourced-postgres)* sequence number handling ([#76](https://github.com/hseeberger/eventsourced/pull/76))

## [0.7.3](https://github.com/hseeberger/eventsourced/compare/eventsourced-postgres-v0.7.2...eventsourced-postgres-v0.7.3) - 2023-11-05

### Other
- more cleanup ([#69](https://github.com/hseeberger/eventsourced/pull/69))
- minor code style cleanup ([#68](https://github.com/hseeberger/eventsourced/pull/68))
- separate readme for each crate ([#66](https://github.com/hseeberger/eventsourced/pull/66))
