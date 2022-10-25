SHELL = /bin/bash

cargo-fix:
	cargo fix --allow-dirty

cargo-fmt:
	cargo fmt --all

cargo-lint:
	cargo clippy --workspace --all-targets -- -D warnings

cargo-test:
	cargo test

cargo-udeps:
	cargo +nightly udeps --all-targets

cargo-sort:
	cargo-sort -w

cargo-build:
	cargo build

cargo-build-release:
	cargo build --release

cargo-build-release-optimized:
	cargo build -Z "build-std=std" --release

yaml-lint:
	yamllint -c .yamllint.yaml .

yaml-fmt:
	yamlfmt

validate-openapi:
	swagger-cli validate ./api/openapi.yaml
	openapi-generator validate -i ./api/openapi.yaml

generate-openapi:
	openapi-generator generate -i ./api/openapi.yaml -g typescript-axios -o frontend/src/api

clean:
	cargo clean
	yarn cache clean