.PHONY: fmt test build run docker-build docker-up docker-down

fmt:
	go fmt ./...

test:
	go test ./...

build:
	go build ./...

run:
	go run ./cmd/server

docker-build:
	docker build -t openclaw-bridge:local .

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down
