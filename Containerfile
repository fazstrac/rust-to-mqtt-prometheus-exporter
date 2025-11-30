# Multi-stage Containerfile for rust-to-mqtt-prometheus-exporter
# - Builder stage compiles a release binary
# - Final stage is a minimal Debian slim image with only runtime deps
# Notes:
# - Use `podman build -f Containerfile -t rust-exporter:latest .` to build
# - If you can produce a musl static binary for this project, the final image can be much smaller

# --- Builder stage ---------------------------------------------------------
FROM rust:1.91-bullseye AS builder

# Install common system build dependencies (adjust if your deps require others)
RUN apt-get update && \
    apt-get install -y --no-install-recommends pkg-config libssl-dev ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

# Copy manifest first and build a dummy binary to populate the cargo registry/cache
# This speeds up rebuilds when only your application code changes.
COPY Cargo.toml Cargo.lock ./

# Create a dummy main to allow dependency-only build
RUN mkdir -p src && echo 'fn main() { println!("placeholder"); }' > src/main.rs
RUN cargo build --release || true

# Replace with real source and build. Update the timestamp of main.rs to ensure recompilation.
COPY . .
RUN touch src/main.rs && cargo build --release

# --- Runtime stage ---------------------------------------------------------
FROM debian:bookworm-slim

# Install only runtime dependencies (CA certs for HTTPS etc.)
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Create an unprivileged user to run the service
RUN useradd -m -u 1000 appuser || true

# Copy the compiled binary from the builder stage
# Adjust the binary name if your crate outputs a different filename.
COPY --from=builder /usr/src/app/target/release/rust-to-mqtt-prometheus-exporter /usr/local/bin/rust-exporter
RUN chown appuser:appuser /usr/local/bin/rust-exporter

USER appuser
WORKDIR /home/appuser

# Minimal environment defaults; override with `--env` or `--env-file` at runtime
ENV RUST_LOG=info

CMD ["/usr/local/bin/rust-exporter"]
