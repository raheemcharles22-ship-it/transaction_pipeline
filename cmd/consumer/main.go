package main

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	w := &kafka.Writer{
		Addr:                   kafka.TCP("localhost:19092"),
		Topic:                  "transactions",
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: true,
	}
	defer w.Close()

	err := w.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte("merchant-1"),
		Value: []byte(`{"id":"test-1"}`),
	})
	if err != nil {
		log.Fatalf("write failed: %v", err)
	}
	log.Println("wrote one message")
}
