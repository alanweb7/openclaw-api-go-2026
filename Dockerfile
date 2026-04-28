# syntax=docker/dockerfile:1

FROM golang:1.22-alpine AS builder
WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/openclaw-bridge ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app

COPY --from=builder /out/openclaw-bridge /app/openclaw-bridge

EXPOSE 8080
ENTRYPOINT ["/app/openclaw-bridge"]
