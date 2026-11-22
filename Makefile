.PHONY: all build run-api run-worker run-scheduler test-integration test test-coverage lint migrate-up migrate-down up down clean

# Variables
DB_URL ?= postgres://notification_user:notification_secret@localhost:5432/notification_db?sslmode=disable

all: lint test build

build:
	mkdir -p bin
	go build -v -o bin/api ./cmd/server
	go build -v -o bin/worker ./cmd/worker
	go build -v -o bin/scheduler ./cmd/scheduler
	go build -v -o bin/cli ./cmd/cli

run-api:
	go run ./cmd/server

run-worker:
	go run ./cmd/worker

run-scheduler:
	go run ./cmd/scheduler

test: test-integration

test-integration:
	docker compose up -d --wait postgres redis
	TEST_DATABASE_URL="$(DB_URL)" TEST_REDIS_ADDR=localhost:6379 go test -count=1 -v -race ./...

test-coverage:
	go test -v -race -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -html=coverage.out -o coverage.html

lint:
	golangci-lint run ./...

migrate-up:
	migrate -path migrations -database "$(DB_URL)" up

migrate-down:
	migrate -path migrations -database "$(DB_URL)" down 1

up:
	docker compose up -d --build --wait

down:
	docker compose down

clean:
	rm -rf bin/ coverage.out coverage.html
