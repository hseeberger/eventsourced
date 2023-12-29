set shell := ["bash", "-uc"]

check:
	cargo check --tests

fmt:
	cargo +nightly fmt

fmt-check:
	cargo +nightly fmt --check

lint:
	cargo clippy --no-deps -- -D warnings

test:
	cargo test

coverage:
	cargo llvm-cov --all-features --workspace --lcov --output-path lcov.info

fix:
	cargo fix --allow-dirty --allow-staged

doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features

all: check fmt lint test doc
