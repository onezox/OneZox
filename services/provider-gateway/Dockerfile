# Post-M2 audit M4: `COPY *.go ./`, not `COPY main.go ./`. Naming the one
# top-level file individually means a second one silently isn't in the
# build context. admin-api's Dockerfile did exactly that and broke the
# moment server.go was added (`undefined: server`). provider-gateway has
# only main.go today — this is prophylactic, and costs nothing.
FROM docker.io/library/golang:1.26-bookworm AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /provider-gateway .

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /provider-gateway /usr/local/bin/provider-gateway
EXPOSE 8080 50051
CMD ["/usr/local/bin/provider-gateway"]
