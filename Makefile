BROKERS ?= localhost:19092
TOPIC ?= transactions
RATE ?= 10
COUNT ?= 0
CHAOS_RATE ?= 0.1

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
	go run ./cmd/consumer