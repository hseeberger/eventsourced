set shell := ["bash", "-uc"]

check:
	cargo check --package eventsourced --all-features
	# cargo check --package eventsourced-nats
	cargo check --package eventsourced-postgres
	cargo check --package eventsourced-projection
	cargo check --package counter
	cargo check --package counter-postgres

fmt toolchain="+nightly":
	cargo {{toolchain}} fmt

fmt-check toolchain="+nightly":
	cargo {{toolchain}} fmt --check

lint:
	cargo clippy --no-deps --all-features -- -D warnings

test:
	cargo test --all-features

fix:
	cargo fix --allow-dirty --allow-staged --all-features

doc toolchain="+nightly":
	RUSTDOCFLAGS="-D warnings --cfg docsrs" cargo {{toolchain}} doc --no-deps --all-features

all: check fmt lint test doc
