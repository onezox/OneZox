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
FROM docker.io/library/golang:1.26-bookworm AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /admin-api .

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /admin-api /usr/local/bin/admin-api
EXPOSE 8080 50051
CMD ["/usr/local/bin/admin-api"]
