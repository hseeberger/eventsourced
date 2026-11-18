# Changelog

## [0.17.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.16.0...eventsourced-nats-v0.17.0) - 2026-11-14

### Changed

- *(deps)* [**breaking**] bump async-nats to 0.51, whose `Client` `make_client` returns

### Other

- *(deps)* bump Rust to 1.100.0
- rename docker-compose.yaml to compose.yaml
- pin images by digest
- unify docker-compose and read its facts via composed

## [0.16.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.15.1...eventsourced-nats-v0.16.0) - 2026-08-13

### Bug Fixes

- *(test)* Make test helper modules available to unit tests ([#414](https://github.com/hseeberger/eventsourced/pull/414))

### Documentation

- Turn on warnings for missing docs ([#398](https://github.com/hseeberger/eventsourced/pull/398))

### Miscellaneous Tasks

- Standardize project scaffolding and package metadata ([#379](https://github.com/hseeberger/eventsourced/pull/379))

## [0.15.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.15.0...eventsourced-nats-v0.15.1) - 2024-05-06

### Other
- updated the following local packages: eventsourced, eventsourced

## [0.15.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.14.4...eventsourced-nats-v0.15.0) - 2024-04-19

### Features

- Add user-password auth ([#233](https://github.com/hseeberger/eventsourced/pull/233))

### Miscellaneous Tasks

- Rename cmd command and evt event ([#232](https://github.com/hseeberger/eventsourced/pull/232))

## [0.14.4](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.14.3...eventsourced-nats-v0.14.4) - 2024-04-10

### Other
- updated the following local packages: eventsourced, eventsourced

## [0.14.3](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.14.2...eventsourced-nats-v0.14.3) - 2024-03-27

### Other
- update Cargo.toml dependencies

## [0.14.2](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.14.1...eventsourced-nats-v0.14.2) - 2024-03-20

### Miscellaneous Tasks

- Simplify API and get rid of type level programming ([#205](https://github.com/hseeberger/eventsourced/pull/205))

## [0.14.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.14.0...eventsourced-nats-v0.14.1) - 2024-03-17

### Other
- updated the following local packages: eventsourced, eventsourced

## [0.14.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.13.11...eventsourced-nats-v0.14.0) - 2024-03-15

### Miscellaneous Tasks

- Rewrite api ([#192](https://github.com/hseeberger/eventsourced/pull/192))

## [0.13.11](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.13.10...eventsourced-nats-v0.13.11) - 2024-03-11

### Other
- update deps ([#189](https://github.com/hseeberger/eventsourced/pull/189))

## [0.13.10](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.13.9...eventsourced-nats-v0.13.10) - 2024-02-19

### Other
- updated the following local packages: eventsourced, eventsourced

## [0.13.9](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.13.8...eventsourced-nats-v0.13.9) - 2024-02-17

### Miscellaneous Tasks

- Major cleanup ([#177](https://github.com/hseeberger/eventsourced/pull/177))

## [0.13.8](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.13.7...eventsourced-nats-v0.13.8) - 2024-02-02

### Other
- updated the following local packages: eventsourced, eventsourced

## [0.13.7](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.13.6...eventsourced-nats-v0.13.7) - 2024-02-01

### Other
- updated the following local packages: eventsourced, eventsourced

## [0.13.6](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.13.5...eventsourced-nats-v0.13.6) - 2024-01-26

### Other
- updated the following local packages: eventsourced, eventsourced

## [0.13.5](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.13.4...eventsourced-nats-v0.13.5) - 2024-01-23

### Other
- update Cargo.toml dependencies

## [0.13.4](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.13.3...eventsourced-nats-v0.13.4) - 2024-01-16

### Features

- Add projections ([#148](https://github.com/hseeberger/eventsourced/pull/148))

### Refactor

- Clearer EvtLog API ([#147](https://github.com/hseeberger/eventsourced/pull/147))

## [0.13.3](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.13.2...eventsourced-nats-v0.13.3) - 2024-01-12

### Other
- updated the following local packages: eventsourced, eventsourced

## [0.13.2](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.13.1...eventsourced-nats-v0.13.2) - 2024-01-06

### Other
- updated the following local packages: eventsourced, eventsourced

## [0.13.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.13.0...eventsourced-nats-v0.13.1) - 2024-01-04

### Miscellaneous Tasks

- Cleaner sequence number handling ([#130](https://github.com/hseeberger/eventsourced/pull/130))

## [0.13.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.12.0...eventsourced-nats-v0.13.0) - 2024-01-04

### Miscellaneous Tasks

- Cleanup ([#129](https://github.com/hseeberger/eventsourced/pull/129))

## [0.12.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.11.0...eventsourced-nats-v0.12.0) - 2023-12-28

### Features

- No longer support tagging ([#116](https://github.com/hseeberger/eventsourced/pull/116))
- Add logging and tracing ([#114](https://github.com/hseeberger/eventsourced/pull/114))
- Use NonZeroU64 instead of SeqNo ([#111](https://github.com/hseeberger/eventsourced/pull/111))

### Bug Fixes

- Hydrate entities / evtlog after seq_no ([#120](https://github.com/hseeberger/eventsourced/pull/120))

## [0.11.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.10.0...eventsourced-nats-v0.11.0) - 2023-12-22

### Features

- Abstract ID ([#107](https://github.com/hseeberger/eventsourced/pull/107))
- Major API overhaul ([#104](https://github.com/hseeberger/eventsourced/pull/104))
- *(eventsourced-nats)* Credentials ([#102](https://github.com/hseeberger/eventsourced/pull/102))

## [0.10.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.9.0...eventsourced-nats-v0.10.0) - 2023-12-20

### Features

- Add const TYPE_NAME to trait EventSourced ([#94](https://github.com/hseeberger/eventsourced/pull/94))

## [0.9.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.8.5...eventsourced-nats-v0.9.0) - 2023-12-08

### Features

- Introduce entity type ([#90](https://github.com/hseeberger/eventsourced/pull/90))

## [0.8.5](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.8.4...eventsourced-nats-v0.8.5) - 2023-11-30

### Other
- updated the following local packages: eventsourced, eventsourced

## [0.8.4](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.8.3...eventsourced-nats-v0.8.4) - 2023-11-29

### Refactor

- Use ToString instead of Into<String> ([#84](https://github.com/hseeberger/eventsourced/pull/84))

## [0.8.3](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.8.2...eventsourced-nats-v0.8.3) - 2023-11-29

### Other
- updated the following local packages: eventsourced, eventsourced

## [0.8.2](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.8.1...eventsourced-nats-v0.8.2) - 2023-11-24

### Bug Fixes

- *(eventsourced-postgres)* Sequence number handling ([#76](https://github.com/hseeberger/eventsourced/pull/76))

## [0.8.1](https://github.com/hseeberger/eventsourced/compare/v-eventsourced-nats-0.7.1...eventsourced-nats-v0.8.1) - 2023-11-23

### Features

- Verify last sequence number on persisting an event ([#71](https://github.com/hseeberger/eventsourced/pull/71))

### Documentation

- Separate readme for each crate ([#66](https://github.com/hseeberger/eventsourced/pull/66))

### Miscellaneous Tasks

- More cleanup ([#69](https://github.com/hseeberger/eventsourced/pull/69))
- Minor code style cleanup ([#68](https://github.com/hseeberger/eventsourced/pull/68))

## [0.7.2](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.7.1...eventsourced-nats-v0.7.2) - 2023-11-05

### Other
- more cleanup ([#69](https://github.com/hseeberger/eventsourced/pull/69))
- minor code style cleanup ([#68](https://github.com/hseeberger/eventsourced/pull/68))
- separate readme for each crate ([#66](https://github.com/hseeberger/eventsourced/pull/66))

## [0.7.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.7.0...v-eventsourced-nats-0.7.1) - 2023-10-19

### Documentation

- Improve RustDoc, etc. ([#61](https://github.com/hseeberger/eventsourced/pull/61))

### Miscellaneous Tasks

- Cleanup ([#60](https://github.com/hseeberger/eventsourced/pull/60))

## [0.7.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.6.0...eventsourced-nats-v0.7.0) - 2023-10-16

### Bug Fixes

- All things ([#58](https://github.com/hseeberger/eventsourced/pull/58))
- Catch up with moving targets ([#57](https://github.com/hseeberger/eventsourced/pull/57))

### Miscellaneous Tasks

- Misc cleanup ([#59](https://github.com/hseeberger/eventsourced/pull/59))
- Misc small cleanups ([#56](https://github.com/hseeberger/eventsourced/pull/56))

### Other

- Fix typo in README - toml, not yaml (#54)

Fix clippy and nats compilation

A couple of issues identified by the latest clippy lints and a compilation error ([#54](https://github.com/hseeberger/eventsourced/pull/54))

## [0.6.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.5.2...eventsourced-nats-v0.6.0) - 2023-01-31

### Features

- Remove to_seq_no from evts_by_id ([#47](https://github.com/hseeberger/eventsourced/pull/47))

### Bug Fixes

- Msg stream no more waiting for heartbeat ([#48](https://github.com/hseeberger/eventsourced/pull/48))

## [0.5.2](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.5.1...eventsourced-nats-v0.5.2) - 2023-01-26

### Features

- Simplify evts_by_tag with Into<String> for tag ([#44](https://github.com/hseeberger/eventsourced/pull/44))

## [0.5.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.5.0...eventsourced-nats-v0.5.1) - 2023-01-26

### Miscellaneous Tasks

- Try improve release config

### Other

- Revert "chore: try improve release config"

This reverts commit 73383b5f74a7f2848596aab07391512145694a70.

## [0.5.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.4.0...eventsourced-nats-v0.5.0) - 2023-01-26

### Features

- Evts_by_tag query and sequence number from event log ([#42](https://github.com/hseeberger/eventsourced/pull/42))
- Emit single event ([#39](https://github.com/hseeberger/eventsourced/pull/39))
- Remove EvtLog.ids ([#38](https://github.com/hseeberger/eventsourced/pull/38))
- Move spawn method from Entity to EventSourced ([#37](https://github.com/hseeberger/eventsourced/pull/37))

### Bug Fixes

- Make SnapshotStore::save take state instead of reference ([#36](https://github.com/hseeberger/eventsourced/pull/36))

## [0.4.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.3.0...eventsourced-nats-v0.4.0) - 2023-01-17

### Features

- Add ids query to EvtLog ([#35](https://github.com/hseeberger/eventsourced/pull/35))

## [0.3.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.2.1...eventsourced-nats-v0.3.0) - 2023-01-14

### Miscellaneous Tasks

- Consistent config API ([#34](https://github.com/hseeberger/eventsourced/pull/34))

### Other

- RELEASE

## [0.2.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.2.0...eventsourced-nats-v0.2.1) - 2023-01-13

### Bug Fixes

- Proper Error for setup snapshot stores ([#32](https://github.com/hseeberger/eventsourced/pull/32))

## [0.2.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.1.1...eventsourced-nats-v0.2.0) - 2023-01-11

### Features

- Use dedicated error for setup of EvtLog/SStore ([#30](https://github.com/hseeberger/eventsourced/pull/30))

### Refactor

- Further simplify fn signatures ([#29](https://github.com/hseeberger/eventsourced/pull/29))

## [0.1.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-nats-v0.1.0...eventsourced-nats-v0.1.1) - 2023-01-08

### Features

- Snapshot store ([#17](https://github.com/hseeberger/eventsourced/pull/17))
- Infinite Postgres EvtLog::evts_by_id ([#15](https://github.com/hseeberger/eventsourced/pull/15))
- Postgres EvtLog ([#13](https://github.com/hseeberger/eventsourced/pull/13))

### Bug Fixes

- PostgresEvtLog.evts_by_id hangs (fix #25) ([#26](https://github.com/hseeberger/eventsourced/pull/26))
- Make API work in async blocks/functions ([#18](https://github.com/hseeberger/eventsourced/pull/18))

### Refactor

- Different approach to binarization ([#14](https://github.com/hseeberger/eventsourced/pull/14))

### Documentation

- Fix some doc links

### Miscellaneous Tasks

- Finish counter example and README ([#19](https://github.com/hseeberger/eventsourced/pull/19))
