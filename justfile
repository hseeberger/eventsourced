set shell := ["bash", "-uc"]

check:
	cargo check --tests

fmt:
	cargo +nightly fmt

fmt_check:
	cargo +nightly fmt --check

lint:
	cargo clippy --no-deps -- -D warnings

test:
	cargo test

all: fmt check lint test

