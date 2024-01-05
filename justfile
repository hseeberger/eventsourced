set shell := ["bash", "-uc"]

check:
	cargo check --tests --package eventsourced --all-features
	cargo check --tests --package eventsourced-nats
	cargo check --tests --package eventsourced-postgres
	cargo check --tests --package eventsourced-projection

fmt:
	cargo +nightly fmt

fmt-check:
	cargo +nightly fmt --check

lint:
	cargo clippy --no-deps --all-features -- -D warnings

test:
	cargo test --all-features

coverage:
	cargo llvm-cov --workspace --lcov --output-path lcov.info --all-features

fix:
	cargo fix --allow-dirty --allow-staged --all-features

doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features

all: check fmt lint test doc
