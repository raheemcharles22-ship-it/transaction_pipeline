package main

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"path/filepath"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"

	"github.com/raheemcharles/txn-pipeline/internal/db"
	"github.com/raheemcharles/txn-pipeline/internal/event"
)

func TestConsumerIntegration_ChaosScenario(t *testing.T) {
	ctx := context.Background()

	redpandaC, err := redpanda.Run(ctx, "docker.redpanda.com/redpandadata/redpanda:v23.3.3")
	if err != nil {
		t.Fatalf("failed to start redpanda: %v", err)
	}
	defer func() {
		if err := testcontainers.TerminateContainer(redpandaC); err != nil {
			t.Fatalf("failed to terminate redpanda container: %v", err)
		}
	}()

	postgresC, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("transactions"),
		postgres.WithUsername("pipeline"),
		postgres.WithPassword("pipeline"),
		postgres.WithInitScripts(filepath.Join("..", "..", "migrations", "001_init.sql")),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("failed to start postgres: %v", err)
	}
	defer func() {
		if err := testcontainers.TerminateContainer(postgresC); err != nil {
			t.Fatalf("failed to terminate postgres container: %v", err)
		}
	}()

	brokerAddr, err := redpandaC.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("failed to get broker address: %v", err)
	}
	dsn, err := postgresC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get postgres dsn: %v", err)
	}

	// create topics on the ephemeral broker
	conn, err := kafka.Dial("tcp", brokerAddr)
	if err != nil {
		t.Fatalf("failed to dial broker: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("failed to close kafka connection: %v", err)
		}
	}()
	if err := conn.CreateTopics(
		kafka.TopicConfig{Topic: "transactions", NumPartitions: 1, ReplicationFactor: 1},
		kafka.TopicConfig{Topic: "transactions.dlq", NumPartitions: 1, ReplicationFactor: 1},
	); err != nil {
		t.Fatalf("failed to create topics: %v", err)
	}

	// produce a known chaos batch using the real event package, no subprocess needed
	writer := &kafka.Writer{Addr: kafka.TCP(brokerAddr), Topic: "transactions"}
	defer func() {
		if err := writer.Close(); err != nil {
			t.Fatalf("failed to close kafka writer: %v", err)
		}
	}()

	r := rand.New(rand.NewPCG(1, 2)) // fixed seed: deterministic test
	var seenKeys []string
	localSeen := make(map[string]bool)
	var expectedCleanDuplicates int
	const total = 100
	for i := 0; i < total; i++ {
		tx := event.GenerateTransaction(r, []string{"m1", "m2", "m3"}, true, 0.3, &seenKeys)
		payload, err := event.EncodePayload(tx, r, true, 0.3)
		if err != nil {
			t.Fatalf("encode payload: %v", err)
		}

		var decoded map[string]any
		_ = json.Unmarshal(payload, &decoded)
		_, hasMerchantID := decoded["merchant_id"]
		isMalformed := !hasMerchantID

		if !isMalformed {
			if localSeen[tx.IdempotencyKey] {
				expectedCleanDuplicates++
			} else {
				localSeen[tx.IdempotencyKey] = true
			}
		}

		if err := writer.WriteMessages(ctx, kafka.Message{Key: []byte(tx.MerchantID), Value: payload}); err != nil {
			t.Fatalf("failed to write message %d: %v", i, err)
		}
	}
	// run the real consumer against the ephemeral infra
	pool, err := db.NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}
	defer func() {
		pool.Close()
	}()

	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{brokerAddr}, Topic: "transactions", GroupID: "test-consumer"})
	defer func() {
		if err := reader.Close(); err != nil {
			t.Fatalf("failed to close kafka reader: %v", err)
		}
	}()
	dlqWriter := &kafka.Writer{Addr: kafka.TCP(brokerAddr), Topic: "transactions.dlq"}
	defer func() {
		if err := dlqWriter.Close(); err != nil {
			t.Fatalf("failed to close dlq writer: %v", err)
		}
	}()

	runCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	runConsumer(runCtx, reader, pool, dlqWriter)

	// assert: every produced event is accounted for
	var rowCount int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM transactions").Scan(&rowCount); err != nil {
		t.Fatalf("failed to count transactions rows: %v", err)
	}

	dlqConn, err := kafka.Dial("tcp", brokerAddr)
	if err != nil {
		t.Fatalf("failed to dial dlq broker: %v", err)
	}
	defer func() {
		if err := dlqConn.Close(); err != nil {
			t.Fatalf("failed to close dlq connection: %v", err)
		}
	}()
	dlqPartitions, err := dlqConn.ReadPartitions("transactions.dlq")
	if err != nil {
		t.Fatalf("failed to read dlq partitions: %v", err)
	}
	var dlqCount int64
	for _, p := range dlqPartitions {
		conn, err := kafka.DialLeader(ctx, "tcp", brokerAddr, "transactions.dlq", p.ID)
		if err != nil {
			t.Fatalf("dial dlq partition %d: %v", p.ID, err)
		}
		first, last, err := conn.ReadOffsets()
		if err := conn.Close(); err != nil {
			t.Fatalf("failed to close dlq partition connection for %d: %v", p.ID, err)
		}
		if err != nil {
			t.Fatalf("read offsets for dlq partition %d: %v", p.ID, err)
		}
		dlqCount += last - first
	}

	if int64(rowCount)+dlqCount+int64(expectedCleanDuplicates) != total {
		t.Errorf("conservation check failed: %d rows + %d dlq entries + %d clean duplicates < %d produced",
			rowCount, dlqCount, expectedCleanDuplicates, total)
	}
}
