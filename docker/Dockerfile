FROM node as node
WORKDIR /app
RUN apt-get install -y git
RUN mkdir -p -m 0700 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts
RUN --mount=type=ssh git clone ssh://git@github.com/optiprism-io/frontend.git .
RUN npm i yarn
RUN npm i
RUN yarn build
FROM rust:1.72.0 AS rust
WORKDIR /app
RUN apt-get update && apt-get install -y clang openssl
COPY src ./src
COPY Cargo.toml ./
COPY Cargo.lock ./
RUN rustup default nightly
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/home/root/app/target \
    --mount=type=ssh \
    cargo build --bin optiprism --release

FROM debian:stable-slim AS runtime
RUN groupadd -r optiprism --gid=101 && useradd -r -g optiprism --uid=101 --home-dir=/var/lib/optiprism --shell=/bin/bash optiprism
RUN apt-get update
RUN apt-get install -y openssl ca-certificates locales && apt-get clean
RUN mkdir -p /var/lib/optiprism /var/log/optiprism /etc/optiprism /etc/optiprism/config.d
COPY --from=node /app/dist/* /var/lib/optiprism/frontend
RUN ls -al /var/lib/optiprism
RUN ls -al /var/lib/optiprism/frontend
COPY docker/config.toml /etc/optiprism/config.d/config.toml
RUN chown -R optiprism:optiprism /var/lib/optiprism /var/log/optiprism /etc/optiprism
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
ENV TZ UTC
COPY --from=rust /app/target/release/optiprism /usr/bin/optiprism
RUN chown -R optiprism:optiprism /usr/bin/optiprism
COPY init /var/lib/optiprism/init
EXPOSE 8080
VOLUME /var/lib/optiprism
ENTRYPOINT ["/usr/bin/optiprism"]
