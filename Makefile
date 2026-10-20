run:
	go run ./cmd/server

test:
	go test -race ./...

up:
	docker compose up --build

down:
	docker compose down -v

.PHONY: run test up down
