SHELL = /bin/bash
DEMO_VERSION =$(shell cd src/cmd; cargo get package.version)
DEMO_TAG = v$(DEMO_VERSION)
DEMO_IMAGE = optiprismio/demo:$(DEMO_TAG)

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
	cargo  udeps --all-targets

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
#	$(info building $(DEMO_IMAGE) docker image...)
#ifneq ($(shell docker images -q $(DEMO_IMAGE) 2> /dev/null),)
#	$(error image $(DEMO_IMAGE) already exists)
#endif
	docker buildx build  --ssh default --load --file demo.Dockerfile --platform=linux/arm64 --progress plain -t $(DEMO_IMAGE) .

docker-publish-demo:
	$(pushing pushing $(DEMO_IMAGE) docker image...)
	docker push $(DEMO_IMAGE)

docker-release-demo: docker-build-demo docker-publish-demo
