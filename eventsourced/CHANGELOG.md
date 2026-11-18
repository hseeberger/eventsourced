# Changelog

## [0.27.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.27.0...eventsourced-v0.27.1) - 2026-08-13

### Bug Fixes

- *(test)* Make test helper modules available to unit tests ([#414](https://github.com/hseeberger/eventsourced/pull/414))

### Documentation

- Turn on warnings for missing docs ([#398](https://github.com/hseeberger/eventsourced/pull/398))

### Miscellaneous Tasks

- Standardize project scaffolding and package metadata ([#379](https://github.com/hseeberger/eventsourced/pull/379))

## [0.27.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.26.0...eventsourced-v0.27.0) - 2024-05-06

### Features

- Add CommandEffect::reply to allow commands that don't alter state ([#240](https://github.com/hseeberger/eventsourced/pull/240))

## [0.26.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.25.0...eventsourced-v0.26.0) - 2024-04-19

### Documentation

- Add drawing to README
- Add drawing to eventsourced rustdoc ([#226](https://github.com/hseeberger/eventsourced/pull/226))

### Miscellaneous Tasks

- Rename cmd command and evt event ([#232](https://github.com/hseeberger/eventsourced/pull/232))

## [0.25.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.24.0...eventsourced-v0.25.0) - 2024-04-10

### Features

- Create replies directly out of handle_cmd using closure ([#219](https://github.com/hseeberger/eventsourced/pull/219))

### Documentation

- Adapt to latest API changes ([#215](https://github.com/hseeberger/eventsourced/pull/215))

### Miscellaneous Tasks

- Minor cleanup ([#222](https://github.com/hseeberger/eventsourced/pull/222))
- Use new naming convention for modules ([#213](https://github.com/hseeberger/eventsourced/pull/213))

## [0.24.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.23.0...eventsourced-v0.24.0) - 2024-03-27

### Features

- Add id and evt args to make_reply ([#207](https://github.com/hseeberger/eventsourced/pull/207))

## [0.23.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.22.0...eventsourced-v0.23.0) - 2024-03-20

### Refactor

- Use trait-variant rewrite

### Miscellaneous Tasks

- Simplify API and get rid of type level programming ([#205](https://github.com/hseeberger/eventsourced/pull/205))

## [0.22.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.21.0...eventsourced-v0.22.0) - 2024-03-18

### Features

- *(eventsourced)* Pass entity id late on spawn ([#199](https://github.com/hseeberger/eventsourced/pull/199))

## [0.21.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.20.1...eventsourced-v0.21.0) - 2024-03-15

### Miscellaneous Tasks

- Rewrite api ([#192](https://github.com/hseeberger/eventsourced/pull/192))

## [0.20.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.20.0...eventsourced-v0.20.1) - 2024-03-11

### Bug Fixes

- Hanging evts_by_id stream on replaying events ([#193](https://github.com/hseeberger/eventsourced/pull/193))

## [0.20.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.19.0...eventsourced-v0.20.0) - 2024-02-19

### Miscellaneous Tasks

- Cleanup ([#180](https://github.com/hseeberger/eventsourced/pull/180))

## [0.19.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.18.0...eventsourced-v0.19.0) - 2024-02-17

### Documentation

- Some cleanup, activate step in GH workflows again ([#175](https://github.com/hseeberger/eventsourced/pull/175))

### Miscellaneous Tasks

- Major cleanup ([#177](https://github.com/hseeberger/eventsourced/pull/177))
- Use error-ext crate ([#169](https://github.com/hseeberger/eventsourced/pull/169))

## [0.18.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.17.0...eventsourced-v0.18.0) - 2024-02-02

### Miscellaneous Tasks

- Rework handle_cmd error ([#167](https://github.com/hseeberger/eventsourced/pull/167))

## [0.17.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.16.0...eventsourced-v0.17.0) - 2024-02-01

### Refactor

- Error_chain no longer pub ([#164](https://github.com/hseeberger/eventsourced/pull/164))

## [0.16.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.15.1...eventsourced-v0.16.0) - 2024-01-26

### Bug Fixes

- Projection run adds one to stored seq_no ([#159](https://github.com/hseeberger/eventsourced/pull/159))

## [0.15.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.15.0...eventsourced-v0.15.1) - 2024-01-23

### Other
- update Cargo.toml dependencies

## [0.15.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.14.2...eventsourced-v0.15.0) - 2024-01-16

### Features

- Add projections ([#148](https://github.com/hseeberger/eventsourced/pull/148))

### Refactor

- Clearer EvtLog API ([#147](https://github.com/hseeberger/eventsourced/pull/147))

## [0.14.2](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.14.1...eventsourced-v0.14.2) - 2024-01-12

### Documentation

- Feature aware docs ([#143](https://github.com/hseeberger/eventsourced/pull/143))

### Miscellaneous Tasks

- Remove obsolete pin-project-lite dep

## [0.14.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.14.0...eventsourced-v0.14.1) - 2024-01-06

### Miscellaneous Tasks

- Print error chains ([#133](https://github.com/hseeberger/eventsourced/pull/133))

## [0.14.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.13.0...eventsourced-v0.14.0) - 2024-01-04

### Miscellaneous Tasks

- Cleaner sequence number handling ([#130](https://github.com/hseeberger/eventsourced/pull/130))

## [0.12.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.11.0...eventsourced-v0.12.0) - 2023-12-28

### Features

- No longer support tagging ([#116](https://github.com/hseeberger/eventsourced/pull/116))
- Add logging and tracing ([#114](https://github.com/hseeberger/eventsourced/pull/114))
- Use NonZeroU64 instead of SeqNo ([#111](https://github.com/hseeberger/eventsourced/pull/111))

### Bug Fixes

- Hydrate entities / evtlog after seq_no ([#120](https://github.com/hseeberger/eventsourced/pull/120))

## [0.11.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.10.0...eventsourced-v0.11.0) - 2023-12-22

### Features

- Abstract ID ([#107](https://github.com/hseeberger/eventsourced/pull/107))
- Major API overhaul ([#104](https://github.com/hseeberger/eventsourced/pull/104))

### Miscellaneous Tasks

- Use trait_variant::make ([#106](https://github.com/hseeberger/eventsourced/pull/106))
- Remove obsolete comment ([#101](https://github.com/hseeberger/eventsourced/pull/101))

## [0.10.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.9.0...eventsourced-v0.10.0) - 2023-12-20

### Features

- Add const TYPE_NAME to trait EventSourced ([#94](https://github.com/hseeberger/eventsourced/pull/94))

## [0.9.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.8.5...eventsourced-v0.9.0) - 2023-12-08

### Features

- Introduce entity type ([#90](https://github.com/hseeberger/eventsourced/pull/90))

### Other

- Fix examples link in eventsourced/README.md ([#88](https://github.com/hseeberger/eventsourced/pull/88))

## [0.8.5](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.8.4...eventsourced-v0.8.5) - 2023-11-30

### Features

- Add id parameter to EventSourced::handle_cmd ([#86](https://github.com/hseeberger/eventsourced/pull/86))

## [0.8.4](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.8.3...eventsourced-v0.8.4) - 2023-11-29

### Refactor

- Use ToString instead of Into<String> ([#84](https://github.com/hseeberger/eventsourced/pull/84))

## [0.8.3](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.8.2...eventsourced-v0.8.3) - 2023-11-29

### Other
- *(eventsourced)* rename feature serde-json serde_json ([#82](https://github.com/hseeberger/eventsourced/pull/82))

## [0.8.2](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.8.1...eventsourced-v0.8.2) - 2023-11-24

### Bug Fixes

- *(eventsourced-postgres)* Sequence number handling ([#76](https://github.com/hseeberger/eventsourced/pull/76))

## [0.8.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.7.2...eventsourced-v0.8.1) - 2023-11-23

### Features

- Verify last sequence number on persisting an event ([#71](https://github.com/hseeberger/eventsourced/pull/71))

## [0.7.2](https://github.com/hseeberger/eventsourced/compare/v-eventsourced-0.7.1...eventsourced-v0.7.2) - 2023-11-05

### Documentation

- Separate readme for each crate ([#66](https://github.com/hseeberger/eventsourced/pull/66))

### Miscellaneous Tasks

- More cleanup ([#69](https://github.com/hseeberger/eventsourced/pull/69))
- Minor code style cleanup ([#68](https://github.com/hseeberger/eventsourced/pull/68))

## [0.7.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.7.0...v-eventsourced-0.7.1) - 2023-10-19

### Documentation

- Improve RustDoc, etc. ([#61](https://github.com/hseeberger/eventsourced/pull/61))

### Miscellaneous Tasks

- Cleanup ([#60](https://github.com/hseeberger/eventsourced/pull/60))

## [0.7.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.6.0...eventsourced-v0.7.0) - 2023-10-16

### Bug Fixes

- All things ([#58](https://github.com/hseeberger/eventsourced/pull/58))

### Documentation

- README fixes
- More README content, some code doc fixes ([#50](https://github.com/hseeberger/eventsourced/pull/50))
- Update/extend README ([#49](https://github.com/hseeberger/eventsourced/pull/49))

### Miscellaneous Tasks

- Misc cleanup ([#59](https://github.com/hseeberger/eventsourced/pull/59))
- Misc small cleanups ([#56](https://github.com/hseeberger/eventsourced/pull/56))
- Simplify example ([#52](https://github.com/hseeberger/eventsourced/pull/52))

### Other

- Fix typo in README - toml, not yaml (#54)

Fix clippy and nats compilation

A couple of issues identified by the latest clippy lints and a compilation error ([#54](https://github.com/hseeberger/eventsourced/pull/54))

## [0.6.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.5.2...eventsourced-v0.6.0) - 2023-01-31

### Features

- Remove to_seq_no from evts_by_id ([#47](https://github.com/hseeberger/eventsourced/pull/47))

## [0.5.2](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.5.1...eventsourced-v0.5.2) - 2023-01-28

### Bug Fixes

- Correct SeqNo handling during restore ([#45](https://github.com/hseeberger/eventsourced/pull/45))

## [0.5.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.5.0...eventsourced-v0.5.1) - 2023-01-26

### Features

- Simplify evts_by_tag with Into<String> for tag ([#44](https://github.com/hseeberger/eventsourced/pull/44))

## [0.5.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.4.1...eventsourced-v0.5.0) - 2023-01-26

### Features

- Simplify cmd handler with IntoTaggedEvt ([#43](https://github.com/hseeberger/eventsourced/pull/43))

### Other

- Revert "chore: try improve release config"

This reverts commit 73383b5f74a7f2848596aab07391512145694a70.

## [0.4.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.4.0...eventsourced-v0.4.1) - 2023-01-26

### Miscellaneous Tasks

- Try improve release config

## [0.4.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.3.0...eventsourced-v0.4.0) - 2023-01-26

### Features

- Evts_by_tag query and sequence number from event log ([#42](https://github.com/hseeberger/eventsourced/pull/42))
- Evt handler returns unit ([#40](https://github.com/hseeberger/eventsourced/pull/40))
- Emit single event ([#39](https://github.com/hseeberger/eventsourced/pull/39))
- Remove EvtLog.ids ([#38](https://github.com/hseeberger/eventsourced/pull/38))
- Move spawn method from Entity to EventSourced ([#37](https://github.com/hseeberger/eventsourced/pull/37))

### Bug Fixes

- Make SnapshotStore::save take state instead of reference ([#36](https://github.com/hseeberger/eventsourced/pull/36))

## [0.3.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.2.1...eventsourced-v0.3.0) - 2023-01-17

### Features

- Add ids query to EvtLog ([#35](https://github.com/hseeberger/eventsourced/pull/35))

## [0.2.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.1.1...eventsourced-v0.2.0) - 2023-01-11

### Bug Fixes

- Use last_seq_no during restore instead of 42 :-( ([#31](https://github.com/hseeberger/eventsourced/pull/31))

### Refactor

- Further simplify fn signatures ([#29](https://github.com/hseeberger/eventsourced/pull/29))
- Get rid of unnecessary assert_send ([#28](https://github.com/hseeberger/eventsourced/pull/28))
- Simplify EvtLog and SnapshotStore fn signatures ([#27](https://github.com/hseeberger/eventsourced/pull/27))

## [0.1.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-v0.1.0...eventsourced-v0.1.1) - 2023-01-08

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

- Some doc fixes and improvements ([#21](https://github.com/hseeberger/eventsourced/pull/21))

### Miscellaneous Tasks

- Review/contribution from olofwalker ([#23](https://github.com/hseeberger/eventsourced/pull/23))
- Finish counter example and README ([#19](https://github.com/hseeberger/eventsourced/pull/19))
