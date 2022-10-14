FROM rust:1.64.0 AS chef
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY rust/. .
RUN cargo chef prepare  --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN apt-get update && apt-get install -y clang
# Install Rustup
RUN curl https://sh.rustup.rs -sSf | sh -s -- --no-modify-path --default-toolchain none -y
ENV PATH /root/.cargo/bin/:$PATH

RUN rustup self update
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY rust/ .
RUN make build-release

FROM debian:buster-slim AS runtime
WORKDIR app
COPY --from=builder /app/target/release/platform /usr/local/bin
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/platform"]
