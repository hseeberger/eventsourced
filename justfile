set shell := ["bash", "-uc"]

check:
	@echo "using toolchain $RUSTUP_TOOLCHAIN"
	cargo check --tests --all-features

fmt:
	@echo "using toolchain $RUSTUP_TOOLCHAIN"
	cargo fmt

fmt_check:
	@echo "using toolchain $RUSTUP_TOOLCHAIN"
	cargo check

lint:
	@echo "using toolchain $RUSTUP_TOOLCHAIN"
	cargo clippy --no-deps --all-features -- -D warnings

test:
	@echo "using toolchain $RUSTUP_TOOLCHAIN"
	cargo test --all-features

fix:
	@echo "using toolchain $RUSTUP_TOOLCHAIN"
	cargo fix --allow-dirty --allow-staged --all-features

doc:
	@echo "using toolchain $RUSTUP_TOOLCHAIN"
	RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features

all: fmt check lint test doc
