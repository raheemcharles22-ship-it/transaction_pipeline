# Decisions

## Phase 0 — Infra

**Redpanda over Apache Kafka.** Single binary, Kafka-wire-compatible, no Zookeeper to run alongside it. Rejected vanilla Kafka for local dev — same API surface, more moving parts for no benefit at this scale.

**Dual Kafka listener (internal/external).** Containers talk to `redpanda:9092`, host processes talk to `localhost:19092`. Rejected a single listener — the producer/consumer run on the host, not in Docker, so they need an address that resolves outside the compose network.

**Money as integer cents, not float.** Standard practice for currency; floats introduce rounding error that compounds across aggregation.

**`id` and `idempotency_key` as separate fields.** `id` identifies the transaction. `idempotency_key` identifies the delivery attempt. A retried delivery of the same transaction reuses the key but could in principle carry a new `id` — keeping them separate lets the dedup logic reason about delivery attempts independent of the entity itself.

**JSON payloads, no schema registry.** Single-team project, no cross-team schema evolution to coordinate. Would reconsider (Avro/protobuf + registry) if multiple independent producers/consumers existed.

**Idempotency enforced via Postgres unique constraint on `idempotency_key`,** not application-level read-then-write checks. Lets the database catch races between concurrent writers instead of trusting app code to check-then-insert atomically.

## Phase 1 — Producer

**`segmentio/kafka-go` over `franz-go`.** Pure Go, no cgo, simpler API. `franz-go` is faster and more complete but adds complexity this project's scale doesn't need.

**`RequiredAcks: kafka.RequireAll`.** Durability-first — wait for all in-sync replicas before considering a write successful, matching the seriousness of "transaction data," even in a single-broker local setup.

**Partition key = `merchant_id`.** Kafka only guarantees ordering within a partition, never topic-wide. Keying by merchant buys per-merchant ordering without paying for global ordering — the same trade-off made in production event pipelines, not a default accepted blindly.

**Chaos logic split into two functions:** `GenerateTransaction` (reuses a prior idempotency key — transport-level duplicate) and `EncodePayload` (drops a required field — data-level malformation). These are different failure classes with different downstream handling (dedup absorbs one, validation rejects the other), so the generator mirrors that split instead of conflating them.

**`rand` seeded per-process via `NewPCG(rand.Uint64(), rand.Uint64())`, not a fixed seed.** A fixed seed (leftover from test code) caused every fresh process run to replay an identical sequence of "random" data — caught via side-by-side runs producing byte-for-byte identical output. Fixed seeds stay reserved for unit tests, where reproducibility is the point.

**`AllowAutoTopicCreation: false`.** Auto-creation silently produced a 1-partition topic more than once, discarding the intentional 3-partition setup. Topics are created explicitly now, so partition count is never implicit.

**Rate limiting via `time.Ticker`, not a tight loop.** A tight loop measures max throughput; a ticker simulates steady-state traffic. This project needs the latter to test the consumer under realistic load, not burst load.

**Write calls get an explicit `context.WithTimeout`,** derived from the shutdown context. An unbounded write context caused a real hang when topic metadata went stale mid-session — now a stuck write fails loudly after 5s instead of blocking forever.

**CLI flags declared per-binary, not in a shared config package,** even though `--brokers`/`--topic` repeat across producer and consumer. Two repetitions isn't a pattern yet (rule of three) — a shared package would also hide flags from a binary's own `main.go` and `--help` output, which is a real discoverability cost. The DLQ replay CLI (phase 5) needing the same flags would be the trigger to revisit this.

## Phase 2 — Consumer

**`pgx`/`pgxpool` over `database/sql`.** Modern standard driver, built-in connection pooling with sane defaults, faster.

**Idempotent insert via `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING`.** Direct Postgres analog to the DynamoDB conditional write pattern used in Card Accept — let the storage layer enforce the no-duplicates invariant instead of an application-level check that would race under concurrent consumers.

**Permanent vs. transient failure split.** Malformed JSON or a missing required field is a property of the data — it goes to the DLQ immediately, no retry, because retrying garbage doesn't fix it. A Postgres connection error is a property of the moment — it gets retried with backoff before falling back to the DLQ as a last resort. Conflating these either floods the DLQ with things that would've succeeded on retry, or retries forever on data that will never become valid.

**Offset commit only after a message reaches a terminal state** — successful insert, confirmed duplicate, or a *successfully written* DLQ entry. Learned the expensive way: an early version committed the offset even when the DLQ write itself failed, silently losing six messages in one run. Fixed by making the DLQ write's success a precondition for committing, not an assumption.

**At-least-once delivery, not exactly-once.** Kafka's transactional API exists but adds real complexity. Idempotent writes at the database layer make at-least-once safe and sufficient — reprocessing a message is harmless, so there's no need to prevent redelivery in the first place.

**Single consumer group (`GroupID: "txn-consumer"`).** Kafka automatically divides partitions across every consumer sharing a group ID — no manual locking needed to prevent two consumers reading the same partition simultaneously. A different group ID would mean an independent, full copy of the stream, useful only if a second, unrelated consumer needed to see everything (e.g., a future real-time metrics service).

**DLQ messages carry the original, unmodified payload bytes plus an `error-reason` header** — not a re-serialized summary. Replay tooling (phase 5) needs the exact bytes that failed, not a reconstruction of what they might have meant.

## Phase 3 — Observability

**Grafana dashboards provisioned as versioned JSON, not built by hand in the UI.** Same reproducibility principle as `docker-compose.yml` and the migrations — a dashboard that only exists in one person's browser isn't infrastructure. Provisioning depends on two host-side directories actually existing with content in them before the container starts; Docker silently creates empty directories for missing bind-mount sources instead of erroring, so an empty `/var/lib/grafana/dashboards` inside the container doesn't necessarily mean the compose config is wrong — check the host side first.

**Conservation check is three-way (`rows + dlq + clean_duplicates == produced`), not two-way.** A duplicate `idempotency_key` that reaches the consumer with an intact payload gets absorbed by `ON CONFLICT DO NOTHING` — zero rows inserted, correctly logged as "duplicate skipped," and never touching the DLQ because it isn't invalid data. A two-way check (`rows + dlq == produced`) has no bucket for that outcome and will always undercount whenever the chaos generator produces duplicates, which isn't a pipeline bug — it's the two-way check being an incomplete model of the system's real outcomes.

**A generated key only counts as "seen" for duplicate-detection purposes if its payload was NOT malformed.** `EncodePayload`'s chaos path drops `idempotency_key` from the JSON entirely, so if the *first* event to mint a key gets malformed, that key never actually reaches Postgres — a later event reusing the same key is a genuine first insert, not a duplicate. Tracking "seen" on generation alone (regardless of malformation) produces an off-by-N overcount of the duplicate bucket; tracking it only on intact payloads matches what Postgres actually persists.

**Integration test chaos generation uses a fixed RNG seed (`NewPCG(1, 2)`), not a random one.** This is purely for reproducibility of test failures — a fixed seed makes a failing run debuggable against the same numbers every time. The conservation invariant itself (`rows + dlq + clean_duplicates == total`) holds for any seed, since it falls directly out of the three outcomes being mutually exclusive and exhaustive by construction, not out of any particular random sequence.

## Phase 4 — Python Aggregator

**The aggregator reads from Postgres, not Kafka.** The consumer has already validated, deduplicated, and durably persisted every transaction. A second Kafka consumer in the aggregator would mean re-implementing that same idempotency and offset-tracking logic for a service that only needs periodic aggregates, not per-event reactivity — Postgres is already the source of truth, and reading it directly is simpler and sufficient.

**`asyncpg` with hand-written SQL, no ORM.** This service runs a small, fixed set of aggregation queries, not general CRUD. An ORM's abstraction earns its keep when query shapes are unpredictable and numerous; here it would just hide the exact SQL — especially the trailing-average window function — that needs to stay visible and tunable directly.

**Trailing average computed in SQL via a window function; the `3x` anomaly threshold applied in Python.** Keeps a stable, testable data-retrieval query separate from a plain, tunable business rule. Burying the threshold inside the query would make it harder to see and harder to change without touching SQL.

**Postgres `numeric` results (from `avg()`/`sum()` over integer columns) arrive as `decimal.Decimal` via `asyncpg`, not `float`.** Hit this directly: `ANOMALY_THRESHOLD` (a `float`) times a raw `trailing_avg_cents` (a `Decimal`) raises `TypeError` — Python's `decimal` module deliberately refuses to mix `Decimal` with `float` to avoid silent precision loss, though it allows `int`. Fixed by explicitly converting to `float`/`int` immediately after pulling each value out of the row, before any arithmetic or before constructing a Pydantic model from it.

**`testcontainers-python` for integration tests, same principle as `testcontainers-go` in phase 3.** Real Postgres in a container, not a mocked database, seeded with fixed timestamps and known amounts so the trailing-average math is exactly computable by hand and assertable against a precise expected value.

**`asyncpg`'s missing type stubs handled via a single `[[tool.mypy.overrides]]` block in `pyproject.toml`, not per-line `# type: ignore` comments.** One override (`module = "asyncpg.*"`, `ignore_missing_imports = true`) covers every file that imports it, instead of scattering ignore comments across `main.py` and `conftest.py` separately.