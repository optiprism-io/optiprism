FROM node:lts-alpine3.16 as ui
WORKDIR /app
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
COPY ./src/demo/data ./demo_data
COPY ./Cargo.toml ./
COPY ./Cargo.lock ./
RUN cargo build --release
FROM debian:stable-slim AS runtime
COPY --from=rust /app/target/release/optiprism /usr/local/bin
COPY --from=rust /app/demo_data ./demo_data
COPY --from=ui /app/demo_data ./demo_data
EXPOSE 8080
ENTRYPOINT ["optiprism","demo","--demo-data-path","./demo_data"]
