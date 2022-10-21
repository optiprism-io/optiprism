SHELL = /bin/bash

cargo-fix:
	cargo fix --allow-dirty

fmt:
	cargo fmt --all

lint:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test

build:
	cargo build

build-release:
	cargo build --release --target=x86_64-unknown-linux-musl

build-optimized:
	cargo build -Z "build-std=std"  --target x86_64-unknown-linux-musl --release -Z unstable-options