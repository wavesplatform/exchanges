FROM rust:1.75 AS builder
WORKDIR /app

RUN apt-get update && apt-get install -y curl openssl libssl-dev libpq-dev protobuf-compiler
RUN /usr/sbin/update-ca-certificates
RUN rustup component add rustfmt

COPY Cargo.* ./
COPY ./crates ./crates

RUN cargo build --release

FROM debian:12 as runtime
WORKDIR /app

RUN apt-get update \
  && apt-get install -y curl openssl libssl-dev libpq-dev postgresql-client \
  && rm -rf /var/lib/apt/lists/*
RUN /usr/sbin/update-ca-certificates

COPY --from=builder /app/target/release/api ./api
COPY --from=builder /app/target/release/consumer ./consumer
COPY --from=builder /app/target/release/migration ./migration
COPY --from=builder /app/crates/database/migrations ./migrations/

CMD ['./api']
