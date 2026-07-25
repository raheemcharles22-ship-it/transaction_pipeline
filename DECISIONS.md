# Decisions

- Use Docker Compose to run local infrastructure for the transaction pipeline, including Redpanda and Postgres, because it provides a simple, repeatable way to start the full stack on any developer machine.
- Use Postgres 16 with a dedicated database named transactions and a pipeline user for application access because it is a common, reliable relational store for transactional data and keeps the environment isolated from other local databases.
- Mount the migrations directory into the Postgres container so SQL initialization scripts run automatically on startup, which is simpler and more transparent than manually applying schema changes by hand.
- Add an initial SQL migration to create the transactions table with the core fields needed for the event model because it establishes a clear schema early and makes future changes easier to track.
- Use Redpanda as the Kafka-compatible message broker instead of a full Kafka deployment because it offers a similar API and developer experience while being easier to run locally for a small pipeline.
- Expose Redpanda on host port 19092 for local development because it avoids conflicts with other common local services and keeps the broker accessible from the host machine.
- Keep the project’s local developer workflow simple by using Make targets for common commands such as starting and stopping services because that reduces setup friction and makes the workflow easier to follow.
- Model transactions around a generated ID plus an idempotency key that defaults to the same value for normal events, because this keeps single delivery semantics straightforward while still allowing duplicate-retry scenarios to be handled explicitly.
- Support a chaos mode in the event generator so the producer can simulate duplicate keys and malformed payloads during development, which makes it easier to test consumer behavior under imperfect input.
- Prefer deterministic tests for the event package over probabilistic assertions, because they make regressions easier to diagnose and keep the test suite stable across runs.
- Make the transaction generator resilient to nil inputs by providing safe defaults for the random source, merchant list, and seen-key state, because that avoids crashes when the producer is initialized with minimal configuration.
