# Changelog

## [0.6.3](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.6.2...eventsourced-projection-v0.6.3) - 2026-11-19

### Documentation

- Regenerate changelogs with the unified config ([#517](https://github.com/hseeberger/eventsourced/pull/517))

### Miscellaneous Tasks

- Rename docker-compose.yaml to compose.yaml ([#488](https://github.com/hseeberger/eventsourced/pull/488))
- Unify docker-compose and read its facts via composed ([#426](https://github.com/hseeberger/eventsourced/pull/426))


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

## [0.6.2](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.6.1...eventsourced-projection-v0.6.2) - 2026-08-13

### Bug Fixes

- *(test)* Make test helper modules available to unit tests ([#414](https://github.com/hseeberger/eventsourced/pull/414))

### Documentation

- Turn on warnings for missing docs ([#398](https://github.com/hseeberger/eventsourced/pull/398))

### Miscellaneous Tasks

- Standardize project scaffolding and package metadata ([#379](https://github.com/hseeberger/eventsourced/pull/379))

## [0.6.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.6.0...eventsourced-projection-v0.6.1) - 2024-05-06

### Other
- updated the following local packages: eventsourced

## [0.6.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.5.2...eventsourced-projection-v0.6.0) - 2024-04-19

### Miscellaneous Tasks

- Rename cmd command and evt event ([#232](https://github.com/hseeberger/eventsourced/pull/232))

## [0.5.2](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.5.1...eventsourced-projection-v0.5.2) - 2024-04-10

### Other
- updated the following local packages: eventsourced

## [0.5.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.5.0...eventsourced-projection-v0.5.1) - 2024-03-27

### Other
- update Cargo.toml dependencies

## [0.5.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.4.1...eventsourced-projection-v0.5.0) - 2024-03-20

### Refactor

- Use trait-variant rewrite

### Miscellaneous Tasks

- Simplify API and get rid of type level programming ([#205](https://github.com/hseeberger/eventsourced/pull/205))

## [0.4.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.4.0...eventsourced-projection-v0.4.1) - 2024-03-17

### Other
- updated the following local packages: eventsourced

## [0.4.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.3.6...eventsourced-projection-v0.4.0) - 2024-03-15

### Miscellaneous Tasks

- Rewrite api ([#192](https://github.com/hseeberger/eventsourced/pull/192))

## [0.3.6](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.3.5...eventsourced-projection-v0.3.6) - 2024-03-11

### Other
- updated the following local packages: eventsourced

## [0.3.5](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.3.4...eventsourced-projection-v0.3.5) - 2024-02-19

### Other
- updated the following local packages: eventsourced

## [0.3.4](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.3.3...eventsourced-projection-v0.3.4) - 2024-02-17

### Documentation

- Some cleanup, activate step in GH workflows again ([#175](https://github.com/hseeberger/eventsourced/pull/175))

### Miscellaneous Tasks

- Major cleanup ([#177](https://github.com/hseeberger/eventsourced/pull/177))
- Use error-ext crate ([#169](https://github.com/hseeberger/eventsourced/pull/169))

## [0.3.3](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.3.2...eventsourced-projection-v0.3.3) - 2024-02-02

### Other
- updated the following local packages: eventsourced

## [0.3.2](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.3.1...eventsourced-projection-v0.3.2) - 2024-02-01

### Refactor

- Error_chain no longer pub ([#164](https://github.com/hseeberger/eventsourced/pull/164))

## [0.3.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.3.0...eventsourced-projection-v0.3.1) - 2024-01-26

### Bug Fixes

- Projection run adds one to stored seq_no ([#159](https://github.com/hseeberger/eventsourced/pull/159))

## [0.3.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.2.1...eventsourced-projection-v0.3.0) - 2024-01-26

### Refactor

- Rename LoadError to Error

## [0.2.1](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.2.0...eventsourced-projection-v0.2.1) - 2024-01-26

### Bug Fixes

- Projection run respects stored seq_no ([#156](https://github.com/hseeberger/eventsourced/pull/156))

## [0.2.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.1.0...eventsourced-projection-v0.2.0) - 2024-01-23

### Miscellaneous Tasks

- Rename postgres

## [0.1.0](https://github.com/hseeberger/eventsourced/compare/eventsourced-projection-v0.0.1...eventsourced-projection-v0.1.0) - 2024-01-23

### Miscellaneous Tasks

- Rework projection API ([#151](https://github.com/hseeberger/eventsourced/pull/151))

## [0.0.1](https://github.com/hseeberger/eventsourced/releases/tag/eventsourced-projection-v0.0.1) - 2024-01-16

### Features

- Add projections ([#148](https://github.com/hseeberger/eventsourced/pull/148))
