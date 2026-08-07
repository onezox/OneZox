FROM docker.io/library/rust:1.97.1-slim-bookworm AS builder
WORKDIR /build
# protobuf-compiler (protoc): NOT for this repo's own proto/ convention
# (generated Go/Python/Rust stubs are committed, never regenerated at
# Docker build time — see src/pb.rs's own doc comment) — needed because
# the etcd-client crate (Phase-04 Step R) compiles ITS OWN bundled etcd
# API .proto files in its build.rs at cargo-build time, unrelated to
# anything in this repo's proto/ directory. Confirmed live: the build
# failed with "Could not find `protoc`" before this was added.
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev ca-certificates protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/edge-gateway /usr/local/bin/edge-gateway
EXPOSE 8080
CMD ["/usr/local/bin/edge-gateway"]
