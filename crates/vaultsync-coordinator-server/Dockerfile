FROM rust:latest AS builder
WORKDIR /usr/src/vaultsync
COPY . .
RUN cargo build -p vaultsync-coordinator-server --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libsqlite3-0 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/vaultsync/target/release/vaultsync-coordinator-server /usr/local/bin/vaultsync-coordinator-server
EXPOSE 9876
ENTRYPOINT ["vaultsync-coordinator-server"]
