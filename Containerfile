# Multi-stage Containerfile for rust-to-mqtt-prometheus-exporter
# - Builder stage compiles a release binary
# - Final stage is a minimal Debian slim image with only runtime deps
# Notes:
# - Use `podman build -f Containerfile -t rust-exporter:latest .` to build
# - If you can produce a musl static binary for this project, the final image can be much smaller

# --- Builder stage ---------------------------------------------------------
FROM rust:1.91-bullseye AS builder

ARG BUILD_DATE
ARG DUCKDB_VERSION
LABEL org.opencontainers.image.created=$BUILD_DATE

# Install common system build dependencies (adjust if your deps require others)
RUN apt-get update && \
    apt-get install -y --no-install-recommends pkg-config libssl-dev ca-certificates curl wget && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

# Copy manifest first and build a dummy binary to populate the cargo registry/cache
# This speeds up rebuilds when only your application code changes.
COPY Cargo.toml Cargo.lock ./

# Create a dummy main to allow dependency-only build
RUN mkdir -p src && echo 'fn main() { println!("placeholder"); }' > src/main.rs
RUN cargo build --release || true

WORKDIR /scratch
RUN curl -O -L https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/libduckdb-linux-amd64.zip && \
    unzip libduckdb-linux-amd64.zip && \
    rm libduckdb-linux-amd64.zip && \
    cp libduckdb* /usr/local/lib/

ENV LD_LIBRARY_PATH=/usr/local/lib
ENV LIBRARY_PATH=/usr/local/lib

# Replace with real source and build. Update the timestamp of main.rs to ensure recompilation.
WORKDIR /usr/src/app
COPY . .
RUN touch src/main.rs && cargo build --release

# --- Runtime stage ---------------------------------------------------------
FROM debian:bookworm-slim

# Create an unprivileged user to run the service
RUN useradd -m -u 1000 appuser || true

# Copy the compiled binary from the builder stage
# Adjust the binary name if your crate outputs a different filename.
COPY --from=builder /scratch/libduckdb.so /usr/local/lib/libduckdb.so
COPY --from=builder /usr/src/app/target/release/rust-to-mqtt-prometheus-exporter /usr/local/bin/rust-exporter
RUN ldconfig && chown appuser:appuser /usr/local/bin/rust-exporter

USER appuser
WORKDIR /data

# Minimal environment defaults; override with `--env` or `--env-file` at runtime
ENV RUST_LOG=info

CMD ["/usr/local/bin/rust-exporter"]
