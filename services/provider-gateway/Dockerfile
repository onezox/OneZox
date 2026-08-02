FROM docker.io/library/golang:1.26-bookworm AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /provider-gateway .

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /provider-gateway /usr/local/bin/provider-gateway
EXPOSE 8080 50051
CMD ["/usr/local/bin/provider-gateway"]
