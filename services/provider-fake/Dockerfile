FROM docker.io/library/golang:1.26-bookworm AS builder
WORKDIR /build
COPY go.mod ./
COPY main.go ./
RUN CGO_ENABLED=0 go build -o /provider-fake .

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /provider-fake /usr/local/bin/provider-fake
EXPOSE 8080
CMD ["/usr/local/bin/provider-fake"]
