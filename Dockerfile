# syntax=docker/dockerfile:1

# Build the React SPA.
FROM node:22 AS web
WORKDIR /src
COPY frontend/package.json frontend/package-lock.json ./frontend/
RUN cd frontend && npm ci
COPY frontend/ ./frontend/
RUN cd frontend && npm run build

# Build both Go binaries once.
FROM golang:1.26 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/gateway ./cmd/gateway
RUN CGO_ENABLED=0 go build -o /out/worker ./cmd/worker

# Gateway image: static binary + the built SPA.
FROM gcr.io/distroless/static:nonroot AS gateway
WORKDIR /app
COPY --from=builder /out/gateway /app/gateway
COPY --from=web /src/web /app/web
EXPOSE 8080
ENTRYPOINT ["/app/gateway"]

# Worker image.
FROM gcr.io/distroless/static:nonroot AS worker
WORKDIR /app
COPY --from=builder /out/worker /app/worker
EXPOSE 8090
ENTRYPOINT ["/app/worker"]
