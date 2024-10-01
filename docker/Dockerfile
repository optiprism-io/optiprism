FROM node as frontend
WORKDIR /app
RUN apt-get install -y git
RUN mkdir -p -m 0700 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts
RUN --mount=type=ssh git clone ssh://git@github.com/optiprism-io/frontend.git .
RUN npm i yarn --legacy-peer-deps
RUN npm i --legacy-peer-deps
ENV VITE_MOCK_API false
ENV VITE_API_BASE_PATH /api
ENV VITE_API_VERSION v1
RUN yarn build

FROM node as tracker
WORKDIR /app
RUN apt-get install -y git
RUN mkdir -p -m 0700 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts
RUN --mount=type=ssh git clone ssh://git@github.com/optiprism-io/optiprism-js.git .
RUN npm i yarn --legacy-peer-deps
RUN npm i --legacy-peer-deps
RUN yarn build

FROM rust:1.80.0 AS rust
ARG GIT_SHA
ENV GIT_SHA=${GIT_SHA}
WORKDIR /app
RUN apt-get update && apt-get install -y clang openssl protobuf-compiler
COPY src ./src
COPY Cargo.toml ./
RUN rustup default nightly
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/home/root/app/target \
    --mount=type=ssh \
    cargo build --release

FROM debian:stable-slim AS runtime
RUN groupadd -r optiprism --gid=101 && useradd -r -g optiprism --uid=101 --home-dir=/var/lib/optiprism --shell=/bin/bash optiprism
RUN apt-get update
RUN apt-get install -y openssl ca-certificates locales && apt-get clean
RUN mkdir -p /var/lib/optiprism /var/log/optiprism /etc/optiprism /etc/optiprism/config.d /var/lib/optiprism/frontend/assets
COPY --from=frontend /app/dist/index.html /var/lib/optiprism/frontend
COPY --from=frontend /app/dist/* /var/lib/optiprism/frontend/assets
RUN ls -al /var/lib/optiprism/frontend
COPY --from=tracker /app/dist/optiprism-min.umd.js /var/lib/optiprism/frontend/tracker.js
COPY docker/config.toml /etc/optiprism/config.d/config.toml
RUN chown -R optiprism:optiprism /var/lib/optiprism /var/log/optiprism /etc/optiprism
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
ENV TZ UTC
COPY --from=rust /app/target/release/main /usr/bin/optiprism
RUN chown -R optiprism:optiprism /usr/bin/optiprism
COPY init /var/lib/optiprism/init
EXPOSE 8080
VOLUME /var/lib/optiprism
ENTRYPOINT ["/usr/bin/optiprism"]
