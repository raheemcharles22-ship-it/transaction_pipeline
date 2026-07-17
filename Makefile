up:
	docker compose up -d

down:
	docker compose down -v

test:
	go test ./...

run-producer:
	go run ./cmd/producer

run-consumer:
	go run ./cmd/consumer