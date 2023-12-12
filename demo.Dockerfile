FROM rust:1.72.0 AS rust
WORKDIR /app
RUN apt-get update && apt-get install -y clang openssl
COPY ./src ./src
COPY ./Cargo.toml ./
COPY ./Cargo.lock ./
ARG SSH_PRIVATE_KEY
RUN mkdir /root/.ssh/
RUN cat ${SSH_PRIVATE_KEY} > /root/.ssh/id_rsa

RUN rustup default nightly
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/home/root/app/target \
    --mount=type=ssh \
    cargo build --cmd demo --release

FROM debian:stable-slim AS runtime
WORKDIR /app
COPY --from=rust /app/target/release/demo ./
COPY data/demo ./demo_data
EXPOSE 8080
ENTRYPOINT ["/app/demo","shop","--demo-data-path","/app/demo_data"]
