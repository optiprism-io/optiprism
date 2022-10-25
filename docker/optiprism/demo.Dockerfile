FROM rust:1.64.0 AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y clang openssl
COPY ./src ./src
COPY ./src/demo/data ./demo_data
COPY ./Cargo.toml ./
COPY ./Cargo.lock ./
RUN cargo build --release
FROM debian:stable-slim AS runtime
COPY --from=builder /app/target/release/optiprism /usr/local/bin
COPY --from=builder /app/demo_data ./demo_data
EXPOSE 8080
ENTRYPOINT ["optiprism","demo","--demo-data-path","./demo_data"]
