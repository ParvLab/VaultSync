FROM rust:latest AS builder
WORKDIR /usr/src/drift
COPY . .
RUN cargo build -p drift-coordinator-server --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libsqlite3-0 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/drift/target/release/drift-coordinator-server /usr/local/bin/drift-coordinator-server
EXPOSE 9876
ENTRYPOINT ["drift-coordinator-server"]
