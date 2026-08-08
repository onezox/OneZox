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
# Post-M2 audit fix H4 — built from the REPO ROOT, not from
# services/edge-gateway.
#
# The previous form was `COPY Cargo.toml Cargo.lock* ./` with the build
# context set to services/edge-gateway. There is no Cargo.lock in that
# directory — this is a Cargo WORKSPACE and the lockfile lives at the repo
# root — so the glob silently matched nothing and every image was built
# with dependencies resolved fresh from crates.io. With every dependency
# on a caret range, that meant no two builds were guaranteed to produce
# the same binary, and the audited image could not be reproduced.
#
# Note there is no `*` on Cargo.lock below. That is deliberate: if the
# lockfile is ever missing, this COPY must FAIL LOUDLY rather than skip,
# which is exactly what the old glob did quietly.
COPY Cargo.toml Cargo.lock ./
COPY services/edge-gateway/Cargo.toml services/edge-gateway/Cargo.toml
COPY services/edge-gateway/src services/edge-gateway/src
# --locked: refuse to build at all if Cargo.lock would need updating.
# Turns "the lockfile drifted" from a silent re-resolution into an error.
RUN cargo build --release --locked -p edge-gateway

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/edge-gateway /usr/local/bin/edge-gateway
EXPOSE 8080
CMD ["/usr/local/bin/edge-gateway"]
