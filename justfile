set shell := ["bash", "-uc"]

nightly := `rustc --version | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' | sed 's/^/nightly-/'`

check:
    cargo check --package eventsourced --all-features --all-targets
    cargo check --package eventsourced-nats --all-targets
    cargo check --package eventsourced-postgres --all-targets
    cargo check --package eventsourced-projection --all-targets
    cargo check --package counter --all-targets
    cargo check --package counter-nats --all-targets
    cargo check --package counter-postgres --all-targets
    cargo check --workspace --all-targets

fix:
    cargo fix --allow-dirty --allow-staged --all-features

fmt:
    cargo +{{ nightly }} fmt
    RUST_LOG=error taplo fmt

fmt-check:
    cargo +{{ nightly }} fmt --check

lint:
    cargo clippy --no-deps --all-features -- -D warnings

lint-fix:
    cargo clippy --no-deps --all-features --fix --allow-dirty --allow-staged

test:
    cargo test --all-features

doc:
    RUSTDOCFLAGS="-D warnings --cfg docsrs" cargo +{{ nightly }} doc --no-deps --all-features

all: check fmt lint test doc
