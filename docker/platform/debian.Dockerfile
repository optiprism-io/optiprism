FROM rust:1.64.0 AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y clang openssl
COPY rust .
RUN cargo build --release

FROM debian:stable-slim AS runtime
COPY --from=builder /app/target/release/platform /usr/local/bin
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/platform"]