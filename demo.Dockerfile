FROM node:lts-alpine3.16 as ui
WORKDIR /app
ENV VITE_API_BASE_PATH=/api
COPY frontend/src frontend/src
COPY frontend/index.html frontend/index.html
COPY frontend/public frontend/public
COPY package.json .
COPY tsconfig.json .
COPY vite.config.ts .
COPY yarn.lock .
RUN yarn install
RUN yarn build

FROM rust:1.64.0 AS rust
WORKDIR /app
RUN apt-get update && apt-get install -y clang openssl
COPY ./src ./src
COPY ./Cargo.toml ./
COPY ./Cargo.lock ./
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/home/root/app/target \
    cargo build --bin demo --release

FROM debian:stable-slim AS runtime
WORKDIR /app
COPY --from=ui /app/frontend/dist ./ui
COPY --from=rust /app/target/release/demo ./
COPY demo/data ./demo_data
EXPOSE 8080
ENTRYPOINT ["/app/demo","--demo-data-path","/app/demo_data","--ui-path","/app/ui"]
