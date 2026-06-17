# syntax=docker/dockerfile:1

# Build both binaries once.
FROM golang:1.26 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/gateway ./cmd/gateway
RUN CGO_ENABLED=0 go build -o /out/worker ./cmd/worker

# Gateway image: static binary + the demo web assets it serves.
FROM gcr.io/distroless/static:nonroot AS gateway
WORKDIR /app
COPY --from=builder /out/gateway /app/gateway
COPY web /app/web
EXPOSE 8080
ENTRYPOINT ["/app/gateway"]

# Worker image.
FROM gcr.io/distroless/static:nonroot AS worker
WORKDIR /app
COPY --from=builder /out/worker /app/worker
EXPOSE 8090
ENTRYPOINT ["/app/worker"]
