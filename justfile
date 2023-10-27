set shell := ["bash", "-uc"]

check:
	@echo "using toolchain ${RUSTUP_TOOLCHAIN:-NONE}"
	cargo check --tests --package eventsourced --all-features
	cargo check --tests --package eventsourced-nats
	cargo check --tests --package eventsourced-postgres

fmt:
	@echo "using toolchain ${RUSTUP_TOOLCHAIN:-NONE}"
	cargo fmt

fmt_check:
	@echo "using toolchain ${RUSTUP_TOOLCHAIN:-NONE}"
	cargo check

lint:
	@echo "using toolchain ${RUSTUP_TOOLCHAIN:-NONE}"
	cargo clippy --no-deps --all-features -- -D warnings

test:
	@echo "using toolchain ${RUSTUP_TOOLCHAIN:-NONE}"
	cargo test --all-features

fix:
	@echo "using toolchain ${RUSTUP_TOOLCHAIN:-NONE}"
	cargo fix --allow-dirty --allow-staged --all-features

doc:
	@echo "using toolchain ${RUSTUP_TOOLCHAIN:-NONE}"
	RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features

all: check fmt lint test doc
