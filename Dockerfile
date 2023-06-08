FROM rust:1.67 AS builder
WORKDIR /app

RUN apt-get update && apt-get install -y curl openssl libssl-dev libpq-dev protobuf-compiler
RUN /usr/sbin/update-ca-certificates
RUN rustup component add rustfmt

COPY Cargo.* ./
COPY ./crates ./crates

RUN cargo install --path ./crates/database
RUN cargo install --path ./crates/exchanges-api
RUN cargo install --path ./crates/exchanges-consumer

FROM debian:11 as runtime
WORKDIR /app

RUN apt-get update \
  && apt-get install -y curl openssl libssl-dev libpq-dev postgresql-client \
  && rm -rf /var/lib/apt/lists/*
RUN /usr/sbin/update-ca-certificates

COPY --from=builder /usr/local/cargo/bin/* ./
COPY --from=builder /app/crates/database/migrations ./migrations/

CMD ['./api']