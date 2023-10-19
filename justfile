set shell := ["bash", "-uc"]

check:
	cargo check --tests --all-features

fmt:
	cargo +nightly fmt

fmt_check:
	cargo +nightly fmt --check

lint:
	cargo clippy --no-deps --all-features -- -D warnings

test:
	cargo test --all-features

doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features

all: fmt check lint test doc
