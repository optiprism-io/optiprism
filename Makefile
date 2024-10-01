$(eval GIT_TAG := $(shell git rev-parse --short HEAD))
SHELL = /bin/bash
VERSION =$(shell cd src/cmd; cargo get package.version)
DEMO_TAG = v$(VERSION)
IMAGE = optiprismio/optiprism:$(DEMO_TAG)
cargo-fix-fmt: cargo-fix cargo-fmt cargo-sort cargo-lint

cargo-fix:
	cargo fix --allow-dirty

cargo-fmt:
	cargo fmt --all

cargo-lint:
	cargo clippy --workspace --all-targets -- -D warnings

cargo-test:
	cargo nextest run -- --test-threads=1

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

clean:
	cargo clean
	yarn cache clean

docker-build:
	docker buildx build  --build-arg="GIT_SHA=$(GIT_TAG)" --ssh default --load --file docker/Dockerfile --platform=linux/amd64 --progress plain -t $(IMAGE) .

docker-publish:
	$(pushing pushing $(IMAGE) docker image...)
	docker push $(IMAGE)

docker-release: docker-build docker-publish

docker-local-build:
	docker buildx build  --build-arg="GIT_SHA=$(GIT_TAG)" --ssh default --load --file docker/local.Dockerfile --platform=linux/amd64 --progress plain -t $(IMAGE) .

docker-local-publish:
	$(pushing pushing $(IMAGE) docker image...)
	docker push $(IMAGE)

docker-local-release: docker-local-build docker-local-publish


release:
	helm upgrade --install --values ./helm/optiprism/values.yaml --set image.tag=v$(VERSION) --set podAnnotations.version=$(VERSION) optiprism ./helm/optiprism -n optiprism