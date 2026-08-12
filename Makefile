BROKERS ?= localhost:19092
TOPIC ?= transactions
RATE ?= 10
COUNT ?= 0
CHAOS_RATE ?= 0.1
DB_DSN ?= postgres://pipeline:pipeline@localhost:5432/transactions


up:
	docker compose up -d

down:
	docker compose down -v

test:
	go test ./...

run-producer:
	go run ./cmd/producer \
		--brokers=$(BROKERS) \
		--topic=$(TOPIC) \
		--rate=$(RATE) \
		--count=$(COUNT) \
		--chaos-rate=$(CHAOS_RATE) \
		$(CHAOS)

run-producer-chaos:
	$(MAKE) run-producer CHAOS=--chaos

run-consumer:
	go run ./cmd/consumer \
		--brokers=$(BROKERS) \
		--topic=$(TOPIC) \
		--db-dsn=$(DB_DSN)

metrics-up:
	docker compose up -d prometheus grafana

run-aggregator:
	cd aggregator && uvicorn main:app --reload --port 8000

test-aggregator:
	cd aggregator && pytest	