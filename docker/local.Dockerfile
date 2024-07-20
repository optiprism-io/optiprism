FROM rust:1.72.0 AS rust
WORKDIR /app
RUN apt-get update && apt-get install -y clang openssl
COPY src ./src
COPY Cargo.toml ./
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
COPY frontend/dist /var/lib/optiprism/frontend
COPY optiprism-js/dist/optiprism-min.umd.js /var/lib/optiprism/frontend/tracker.js
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
