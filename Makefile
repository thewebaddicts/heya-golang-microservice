.PHONY: run test build

run:
	go run ./cmd/heya-golang-microservice

test:
	go test ./...

build:
	go build -o bin/heya-golang-microservice ./cmd/heya-golang-microservice
