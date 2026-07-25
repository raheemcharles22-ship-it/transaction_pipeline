package main

import (
	"context"
	"flag"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/raheemcharles/txn-pipeline/internal/event"
	"github.com/segmentio/kafka-go"
)

func produceLoop(ctx context.Context, rate int, count int, produce func()) {
	interval := time.Second / time.Duration(rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	n := 0
	for {
		select {
		case <-ctx.Done():
			log.Println("shutdown signal received, draining...")
			return
		case <-ticker.C:
			produce()
			n++
			if count > 0 && n >= count {
				return
			}
		}
	}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	rate := flag.Int("rate", 10, "events per second")
	count := flag.Int("count", 0, "total number of events to produce (0 for infinite)")
	chaos := flag.Bool("chaos", false, "enable chaos mode (duplicate keys and bad payloads)")
	chaosRate := flag.Float64("chaos-rate", 0.1, "fraction of events to be chaotic (0.0 to 1.0)")
	brokers := flag.String("brokers", "localhost:19092", "Kafka broker addresses (comma-separated)")
	topic := flag.String("topic", "transactions", "Kafka topic to produce to")
	merchants := flag.String("merchants", "m1,m2,m3", "comma-separated list of merchant IDs to simulate")
	flag.Parse()

	merchantIDs := strings.Split(*merchants, ",")
	now := time.Now().UnixNano()
	r := rand.New(rand.NewPCG(uint64(now), uint64(now)^0xA5A5A5A5A5A5A5A5))
	var seenKeys []string // created once, grows across calls

	w := &kafka.Writer{
		Addr:                   kafka.TCP(*brokers),
		Topic:                  *topic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false,
	}
	defer func() {
		if err := w.Close(); err != nil {
			log.Printf("error closing writer: %v", err)
		}
	}()

	produceLoop(ctx, *rate, *count, func() {
		tx := event.GenerateTransaction(r, merchantIDs, *chaos, *chaosRate, &seenKeys)
		payload, err := event.EncodePayload(tx, r, *chaos, *chaosRate)
		if err != nil {
			log.Printf("encode error: %v", err)
			return
		}
		if err := w.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(tx.MerchantID),
			Value: payload,
		}); err != nil {
			log.Printf("write error: %v", err)
		}
	})

	log.Println("producer shut down cleanly")
}
