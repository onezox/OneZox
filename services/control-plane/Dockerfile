# Phase-04 Step D. Same multi-stage shape as provider-gateway's Dockerfile.
#
# P03 lesson (Phase-03-Complete.txt): data-plane's Dockerfile silently
# never shipped four of its own Python modules, because its COPY lines
# enumerated each top-level module directory individually and were never
# kept in sync as new ones were added. `COPY internal ./internal` below is
# a single blanket copy of the whole internal/ tree — structurally immune
# to that exact bug, since there's no per-subdirectory line to forget when
# registry/rollout/quota/policy/pricing grow real implementations in later
# steps. Also, unlike Python's dynamic imports, `go build .` fails loud at
# BUILD time (not runtime) if a package main.go imports is missing from
# the build context — a second, independent layer against the same class
# of gap. Confirmed live (Step D): `find /build/internal` inside the
# builder stage shows all five module directories plus pb/ actually
# present in the build context, not just the repo.
#
# Post-M2 audit M4: `COPY *.go ./`, not `COPY main.go ./`. The blanket
# internal/ copy above closed this bug class for SUBDIRECTORIES but left
# it open one level up — a second top-level .go file would simply not be
# in the build context. That is not hypothetical: admin-api's Dockerfile
# named main.go individually at Step E and broke at Step H the moment
# server.go was added (`undefined: server`). control-plane has only
# main.go today, so this is prophylactic — which is the point. The fix
# costs nothing now and removes the trap before someone walks into it.
FROM docker.io/library/golang:1.26-bookworm AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /control-plane .

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /control-plane /usr/local/bin/control-plane
EXPOSE 8080 50051
CMD ["/usr/local/bin/control-plane"]
