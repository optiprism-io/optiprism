FROM rust:alpine3.16 AS builder
WORKDIR /app
RUN apk add --update --no-cache clang clang-libs clang-dev make bash musl-dev pkgconfig openssl openssl-dev
# Build application
COPY rust/ .
RUN make build-release

FROM alpine:3.16 AS runtime
WORKDIR app
COPY --from=builder /app/target/release/platform /usr/local/bin
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/platform"]
