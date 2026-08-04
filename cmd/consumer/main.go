package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/raheemcharles/txn-pipeline/internal/db"
	"github.com/raheemcharles/txn-pipeline/internal/event"
	"github.com/segmentio/kafka-go"
)

var (
	eventsProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "txn_events_processed_total", Help: "Events processed by Outcome"},
		[]string{"outcome"}, //dlq, inserted, duplicated
	)

	processingLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{Name: "txn_processing_duration_seconds", Help: "time from fetching message to commit"},
	)

	consumerLag = prometheus.NewGauge(
		prometheus.GaugeOpts{Name: "txn_consumer_lag", Help: "Messages behind the latest offset"},
	)
)

func init() {
	prometheus.MustRegister(eventsProcessed, processingLatency, consumerLag)
}

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

func processMessage(ctx context.Context, m kafka.Message, r *kafka.Reader, pool *pgxpool.Pool, dlqWriter *kafka.Writer) {
	timer := prometheus.NewTimer(processingLatency)
	defer timer.ObserveDuration()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	tx, reason, ok := classifyTransaction(m.Value)
	if !ok {
		if dlqErr := sendToDLQ(ctx, dlqWriter, m, reason); dlqErr != nil {
			logger.Info("dlq write failed; leaving uncommitted for redelivery", "error", dlqErr)
			return
		}
		eventsProcessed.WithLabelValues("dlq").Inc()
		if err := r.CommitMessages(ctx, m); err != nil {
			logger.Error("error committing message offset", "error", err)
		}
		return
	}

	inserted, err := insertWithRetry(ctx, pool, tx)
	if err != nil {
		if dlqErr := sendToDLQ(ctx, dlqWriter, m, "db insert error: "+err.Error()); dlqErr != nil {
			logger.Error("dlq write failed; leaving uncommitted for redelivery", "error", dlqErr)
			return
		}
		eventsProcessed.WithLabelValues("dlq").Inc()
	} else if inserted {
		eventsProcessed.WithLabelValues("inserted").Inc()
		logger.Info("transaction inserted successfully", "id", tx.ID)
	} else {
		eventsProcessed.WithLabelValues("duplicate").Inc()
		logger.Warn("transaction skipped due to duplicate idempotency key", "idempotency_key", tx.IdempotencyKey)
	}

	if err := r.CommitMessages(ctx, m); err != nil {
		logger.Error("error committing message offset", "error", err)
	}
}

func runConsumer(ctx context.Context, r *kafka.Reader, pool *pgxpool.Pool, dlqWriter *kafka.Writer) {
	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("fetch error: %v", err)
			continue
		}
		processMessage(ctx, m, r, pool, dlqWriter)
	}
}

func main() {

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	brokers := flag.String("brokers", "localhost:19092", "Kafka broker addresses (comma-separated)")
	topic := flag.String("topic", "transactions", "Kafka topic to produce to")
	dbDSN := flag.String("db-dsn", "postgres://pipeline:pipeline@localhost:5432/transactions?sslmode=disable", "Postgres DSN")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	pool, err := db.NewPool(ctx, *dbDSN)
	if err != nil {
		logger.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:         []string{*brokers},
		Topic:           *topic,
		GroupID:         "txn-consumer",
		ReadLagInterval: 1 * time.Second,
	})
	defer func() {
		if err := r.Close(); err != nil {
			logger.Error("error closing reader", "error", err)
		}
	}()

	dlqWriter := &kafka.Writer{
		Addr:  kafka.TCP(*brokers),
		Topic: "transactions.dlq",
	}
	defer func() {
		if err := dlqWriter.Close(); err != nil {
			logger.Error("error closing dlq writer", "error", err)
		}
	}()

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(":2112", nil); err != nil {
			logger.Error("metrics server failed", "error", err)
			os.Exit(1)
		}
	}()

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			consumerLag.Set(float64(r.Stats().Lag))
		}
	}()

	runConsumer(ctx, r, pool, dlqWriter)
}
