FROM docker.io/library/golang:1.26-bookworm AS builder
WORKDIR /build
COPY go.mod main.go ./
RUN go mod tidy && go build -o /provider-stub .

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /provider-stub /usr/local/bin/provider-stub
EXPOSE 8080
CMD ["/usr/local/bin/provider-stub"]
