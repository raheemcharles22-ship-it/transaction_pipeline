package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/raheemcharles/txn-pipeline/internal/db"
	"github.com/raheemcharles/txn-pipeline/internal/event"
	"github.com/segmentio/kafka-go"
)

func sendToDLQ(ctx context.Context, w *kafka.Writer, m kafka.Message, reason string) error {
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err := w.WriteMessages(writeCtx, kafka.Message{
		Key:   m.Key,
		Value: m.Value,
		Headers: []kafka.Header{
			{Key: "error-reason", Value: []byte(reason)},
		},
	})
	if err != nil {
		log.Printf("failed to write to DLQ: %v", err)
	} else {
		log.Printf("routed to DLQ: reason=%s", reason)
	}
	return err
}

func insertWithRetry(ctx context.Context, pool *pgxpool.Pool, tx event.Transaction) (inserted bool, err error) {
	const maxRetries = 3
	for i := 0; i < maxRetries; i++ {
		execCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		tag, execErr := pool.Exec(execCtx, `
			INSERT INTO transactions (id, merchant_id, amount_cents, currency, occurred_at, idempotency_key)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (idempotency_key) DO NOTHING`,
			tx.ID, tx.MerchantID, tx.AmountCents, tx.Currency, tx.Timestamp, tx.IdempotencyKey)
		cancel()

		if execErr == nil {
			return tag.RowsAffected() > 0, nil
		}
		log.Printf("insert attempt %d failed: %v", i+1, execErr)
		time.Sleep(500 * time.Millisecond)
	}
	return false, fmt.Errorf("failed to insert transaction after %d retries", maxRetries)
}

func classifyTransaction(raw []byte) (tx event.Transaction, reason string, ok bool) {
	if err := json.Unmarshal(raw, &tx); err != nil {
		return event.Transaction{}, "malformed json: " + err.Error(), false
	}
	if tx.MerchantID == "" || tx.IdempotencyKey == "" {
		return tx, "missing required field", false
	}
	return tx, "", true
}

func main() {

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	brokers := flag.String("brokers", "localhost:19092", "Kafka broker addresses (comma-separated)")
	topic := flag.String("topic", "transactions", "Kafka topic to produce to")
	dbDSN := flag.String("db-dsn", "postgres://pipeline:pipeline@localhost:5432/transactions?sslmode=disable", "Postgres DSN")
	flag.Parse()

	pool, err := db.NewPool(ctx, *dbDSN)
	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	defer pool.Close()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{*brokers},
		Topic:   *topic,
		GroupID: "txn-consumer",
	})
	defer func() {
		if err := r.Close(); err != nil {
			log.Printf("error closing reader: %v", err)
		}
	}()

	dlqWriter := &kafka.Writer{
		Addr:  kafka.TCP(*brokers),
		Topic: "transactions.dlq",
	}
	defer dlqWriter.Close()

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("shutdown signal received, stopping consumer")
				return
			}
			log.Printf("fetch error: %v", err)
			continue
		}

		var tx event.Transaction

		tx, reason, ok := classifyTransaction(m.Value)
		if !ok {
			if dlqErr := sendToDLQ(ctx, dlqWriter, m, reason); dlqErr != nil {
				log.Printf("DLQ write failed, leaving uncommitted for redelivery: %v", dlqErr)
				continue
			}
			r.CommitMessages(ctx, m)
			continue
		}

		inserted, err := insertWithRetry(ctx, pool, tx)
		if err != nil {
			if dlqErr := sendToDLQ(ctx, dlqWriter, m, "db insert error: "+err.Error()); dlqErr != nil {
				log.Printf("DLQ write failed, leaving uncommitted for redelivery: %v", dlqErr)
				continue // no commit — message gets redelivered and retried
			}
		} else if inserted {
			log.Printf("transaction inserted successfully: %s", tx.ID)
		} else {
			log.Printf("transaction skipped due to duplicate idempotency key: %s", tx.IdempotencyKey)
		}
		r.CommitMessages(ctx, m)

	}

}
