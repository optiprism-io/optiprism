SHELL = /bin/bash

fmt:
	cargo fmt --all

build:
	cargo build

build-release:
	cargo build --release

build-optimized:
	cargo build -Z "build-std=std"  --target x86_64-apple-darwin --release -Z unstable-options