set shell := ["bash", "-uc"]

check:
	@echo "RUSTUP_TOOLCHAIN is ${RUSTUP_TOOLCHAIN:-not set}"
	cargo check --tests --package eventsourced --all-features
	cargo check --tests --package eventsourced-nats
	cargo check --tests --package eventsourced-postgres

fmt:
	@echo "RUSTUP_TOOLCHAIN is ${RUSTUP_TOOLCHAIN:-not set}"
	cargo fmt

fmt-check:
	@echo "RUSTUP_TOOLCHAIN is ${RUSTUP_TOOLCHAIN:-not set}"
	cargo fmt --check

lint:
	@echo "RUSTUP_TOOLCHAIN is ${RUSTUP_TOOLCHAIN:-not set}"
	cargo clippy --no-deps --all-features -- -D warnings

test:
	@echo "RUSTUP_TOOLCHAIN is ${RUSTUP_TOOLCHAIN:-not set}"
	cargo test --all-features

fix:
	@echo "RUSTUP_TOOLCHAIN is ${RUSTUP_TOOLCHAIN:-not set}"
	cargo fix --allow-dirty --allow-staged --all-features

doc:
	@echo "RUSTUP_TOOLCHAIN is ${RUSTUP_TOOLCHAIN:-not set}"
	RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features

all: check fmt lint test doc
