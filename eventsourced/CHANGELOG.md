# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.24.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.23.0...eventsourced-v0.24.0) - 2024-03-27

### Added
- add id and evt args to make_reply ([#207](https://github.com/hseeberger/eventsourced/pull/207))

## [0.23.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.22.0...eventsourced-v0.23.0) - 2024-03-20

### Other
- simplify API and get rid of type level programming ([#205](https://github.com/hseeberger/eventsourced/pull/205))
- use trait-variant rewrite

## [0.22.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.21.0...eventsourced-v0.22.0) - 2024-03-17

### Added
- *(eventsourced)* pass entity id late on spawn ([#199](https://github.com/hseeberger/eventsourced/pull/199))

## [0.21.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.20.1...eventsourced-v0.21.0) - 2024-03-15

### Other
- rewrite api ([#192](https://github.com/hseeberger/eventsourced/pull/192))

## [0.20.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.20.0...eventsourced-v0.20.1) - 2024-03-11

### Fixed
- hanging evts_by_id stream on replaying events ([#193](https://github.com/hseeberger/eventsourced/pull/193))

## [0.20.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.19.0...eventsourced-v0.20.0) - 2024-02-19

### Other
- cleanup ([#180](https://github.com/hseeberger/eventsourced/pull/180))

## [0.19.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.18.0...eventsourced-v0.19.0) - 2024-02-17

### Other
- major cleanup ([#177](https://github.com/hseeberger/eventsourced/pull/177))
- some cleanup, activate step in GH workflows again ([#175](https://github.com/hseeberger/eventsourced/pull/175))
- update deps ([#173](https://github.com/hseeberger/eventsourced/pull/173))
- use error-ext crate ([#169](https://github.com/hseeberger/eventsourced/pull/169))

## [0.18.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.17.0...eventsourced-v0.18.0) - 2024-02-02

### Other
- rework handle_cmd error ([#167](https://github.com/hseeberger/eventsourced/pull/167))

## [0.17.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.16.0...eventsourced-v0.17.0) - 2024-02-01

### Other
- error_chain no longer pub ([#164](https://github.com/hseeberger/eventsourced/pull/164))

## [0.16.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.15.1...eventsourced-v0.16.0) - 2024-01-26

### Fixed
- projection run adds one to stored seq_no ([#159](https://github.com/hseeberger/eventsourced/pull/159))

## [0.15.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.15.0...eventsourced-v0.15.1) - 2024-01-23

### Other
- update Cargo.toml dependencies

## [0.15.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.14.2...eventsourced-v0.15.0) - 2024-01-16

### Added
- add projections ([#148](https://github.com/hseeberger/eventsourced/pull/148))

### Other
- clearer EvtLog API ([#147](https://github.com/hseeberger/eventsourced/pull/147))

## [0.14.2](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.14.1...eventsourced-v0.14.2) - 2024-01-12

### Other
- feature aware docs ([#143](https://github.com/hseeberger/eventsourced/pull/143))
- remove obsolete pin-project-lite dep

## [0.14.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.14.0...eventsourced-v0.14.1) - 2024-01-06

### Other
- print error chains ([#133](https://github.com/hseeberger/eventsourced/pull/133))

## [0.14.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.13.0...eventsourced-v0.14.0) - 2024-01-04

### Other
- cleaner sequence number handling ([#130](https://github.com/hseeberger/eventsourced/pull/130))

## [0.12.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.11.0...eventsourced-v0.12.0) - 2023-12-28

### Added
- no longer support tagging ([#116](https://github.com/hseeberger/eventsourced/pull/116))
- add logging and tracing ([#114](https://github.com/hseeberger/eventsourced/pull/114))
- use NonZeroU64 instead of SeqNo ([#111](https://github.com/hseeberger/eventsourced/pull/111))

### Fixed
- hydrate entities / evtlog after seq_no ([#120](https://github.com/hseeberger/eventsourced/pull/120))

## [0.10.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.9.0...eventsourced-v0.10.0) - 2023-12-20

### Added
- add const TYPE_NAME to trait EventSourced ([#94](https://github.com/hseeberger/eventsourced/pull/94))

## [0.9.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.8.5...eventsourced-v0.9.0) - 2023-12-08

### Added
- introduce entity type ([#90](https://github.com/hseeberger/eventsourced/pull/90))

### Other
- Fix examples link in eventsourced/README.md ([#88](https://github.com/hseeberger/eventsourced/pull/88))

## [0.8.5](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.8.4...eventsourced-v0.8.5) - 2023-11-30

### Added
- add id parameter to EventSourced::handle_cmd ([#86](https://github.com/hseeberger/eventsourced/pull/86))

## [0.8.4](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.8.3...eventsourced-v0.8.4) - 2023-11-29

### Other
- use ToString instead of Into<String> ([#84](https://github.com/hseeberger/eventsourced/pull/84))

## [0.8.3](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.8.2...eventsourced-v0.8.3) - 2023-11-29

### Other
- *(eventsourced)* rename feature serde-json serde_json ([#82](https://github.com/hseeberger/eventsourced/pull/82))

## [0.8.2](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.8.1...eventsourced-v0.8.2) - 2023-11-24

### Fixed
- *(eventsourced-postgres)* sequence number handling ([#76](https://github.com/hseeberger/eventsourced/pull/76))

## [0.7.2](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.7.1...eventsourced-v0.7.2) - 2023-11-05

### Other
- more cleanup ([#69](https://github.com/hseeberger/eventsourced/pull/69))
- minor code style cleanup ([#68](https://github.com/hseeberger/eventsourced/pull/68))
- separate readme for each crate ([#66](https://github.com/hseeberger/eventsourced/pull/66))
- rename feature serde_json/serde-json, improve justfile
