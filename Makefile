SHELL = /bin/bash
VERSION = v$(shell cargo get version)
DEMO_IMAGE = optiprism/optiprism:$(VERSION)-demo

cargo-fix-fmt: cargo-fix cargo-fmt cargo-sort cargo-lint

cargo-fix:
	cargo fix --allow-dirty

cargo-fmt:
	cargo fmt --all

cargo-lint:
	cargo clippy --workspace --all-targets -- -D warnings

cargo-test:
	cargo nextest run

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

docker-build-demo:
ifneq ($(shell docker images -q $(IMAGE) 2> /dev/null),)
	$(error image $(IMAGE) already exists)
endif
	docker buildx build --ssh default --platform=linux/amd64 --progress plain -t $(IMAGE) .

docker-publish-demo:
	docker push $(IMAGE)

docker-release-demo: docker-build-demo docker-publish-demo
