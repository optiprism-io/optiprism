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

clean:
	cargo clean
	yarn cache clean

docker-build:
	docker buildx build  --ssh default --load --file docker/Dockerfile --platform=linux/amd64 --progress plain -t $(IMAGE) .

docker-publish:
	$(pushing pushing $(IMAGE) docker image...)
	docker push $(IMAGE)

docker-release: docker-build docker-publish

docker-backend-build:
	docker buildx build  --no-cache --ssh default --load --file docker/backend.Dockerfile --platform=linux/amd64 --progress plain -t $(IMAGE) .

docker-backend-publish:
	$(pushing pushing $(IMAGE) docker image...)
	docker push $(IMAGE)

docker-backend-release: docker-backend-build docker-backend-publish

docker-frontend-build:
	docker buildx build  --no-cache --ssh default --load --file docker/frontend.Dockerfile --platform=linux/amd64 --progress plain -t $(IMAGE)-frontend .

docker-frontend-publish:
	$(pushing pushing $(IMAGE)-frontend docker image...)
	docker push $(IMAGE)-frontend

docker-frontend-release: docker-frontend-build docker-frontend-publish

release:
	helm upgrade --install --values ./helm/optiprism/values.yaml --set image.tag=v$(VERSION) --set podAnnotations.version=$(VERSION) optiprism ./helm/optiprism -n optiprism