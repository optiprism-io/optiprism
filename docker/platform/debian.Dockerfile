FROM rust:1.64.0 AS builder
WORKDIR /app
ENV TARGET=x86_64-unknown-linux-musl
RUN apt-get update && apt-get install -y clang openssl musl-dev musl
COPY rust .
RUN rustup target add ${TARGET}
RUN cargo build --release --target=${TARGET}

FROM alpine:3.16 AS runtime
ENV TARGET=x86_64-unknown-linux-musl
COPY --from=builder /app/target/${TARGET}/release/platform /usr/local/bin
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/platform"]
