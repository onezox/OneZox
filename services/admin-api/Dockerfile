# Phase-05 Step E. Same multi-stage shape as control-plane's Dockerfile,
# same P03 lesson applied (Phase-03-Complete.txt): `COPY internal ./internal`
# is one blanket copy of the whole internal/ tree, not a per-subdirectory
# line to forget as pb/, graph/, and later auth/rbac/controlclient/ (Steps
# F-L) get added. This service specifically has a codegen'd subtree
# (internal/graph/, gqlgen's own output) that a per-directory COPY list
# would be exactly the kind of thing to silently miss — the same class of
# gap that data-plane's own Dockerfile hit for four straight steps.
# Confirmed live (Step E): `find /build/internal` inside the builder stage
# shows pb/ and graph/ both actually present in the build context, not
# just the repo.
#
# `COPY *.go ./`, not `COPY main.go ./`: Step E's own original version of
# this line named main.go individually and hit the EXACT same class of gap
# one step later, at Step H — adding server.go as a second top-level file
# broke the Docker build (`undefined: server`) because the explicit
# COPY line never mentioned it. Caught immediately by the build itself
# failing loud (Go's own "undefined" compile error), not silently, but
# it's the same lesson this file's own header already warned about,
# recurring at a different scope (top-level files, not just internal/
# subdirectories) — fixed here with a wildcard for the same reason
# internal/ already gets a blanket copy: no per-file line left to forget
# as more top-level files are added.
FROM docker.io/library/golang:1.26-bookworm AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /admin-api .

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /admin-api /usr/local/bin/admin-api
EXPOSE 8080 50051
CMD ["/usr/local/bin/admin-api"]
