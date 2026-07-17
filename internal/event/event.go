package event

import "time"

type Transaction struct {
	ID             string    `json:"id"`
	MerchantId     string    `json:"merchant_id"`
	AmountCents    int64     `json:"amount_cents"`
	Currency       string    `json:"currency"`
	Timestamp      time.Time `json:"timestamp"`
	IdempotencyKey string    `json:"idempotency_key"`
}
