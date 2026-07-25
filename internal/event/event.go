package event

import (
	"encoding/json"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
)

type Transaction struct {
	ID             string    `json:"id"`
	MerchantID     string    `json:"merchant_id"`
	AmountCents    int64     `json:"amount_cents"`
	Currency       string    `json:"currency"`
	Timestamp      time.Time `json:"timestamp"`
	IdempotencyKey string    `json:"idempotency_key"`
}

// GenerateTransaction builds one transaction. When chaos is true, chaosRate
// fraction of events reuse a prior idempotency key from seenKeys — simulating
// a duplicate/redelivered message.
func GenerateTransaction(r *rand.Rand, merchantIDs []string, chaos bool, chaosRate float64, seenKeys *[]string) Transaction {
	if r == nil {
		r = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	}
	if len(merchantIDs) == 0 {
		merchantIDs = []string{"unknown-merchant"}
	}
	if seenKeys == nil {
		var local []string
		seenKeys = &local
	}

	id := uuid.NewString()
	idemKey := id

	if chaos && len(*seenKeys) > 0 && r.Float64() < chaosRate {
		idemKey = (*seenKeys)[r.IntN(len(*seenKeys))] // reuse an old key
	} else {
		*seenKeys = append(*seenKeys, idemKey)
	}

	return Transaction{
		ID:             id,
		MerchantID:     merchantIDs[r.IntN(len(merchantIDs))],
		AmountCents:    int64(r.IntN(100_000) + 1),
		Currency:       "USD",
		Timestamp:      time.Now(),
		IdempotencyKey: idemKey,
	}
}

// EncodePayload serializes tx to JSON. When chaos is true, chaosRate fraction
// of payloads drop a required field — simulating bad data hitting the consumer.
func EncodePayload(tx Transaction, r *rand.Rand, chaos bool, chaosRate float64) ([]byte, error) {
	if chaos && r.Float64() < chaosRate {
		return json.Marshal(map[string]any{
			"id":           tx.ID,
			"amount_cents": tx.AmountCents,
			// merchant_id and idempotency_key omitted on purpose
		})
	}
	return json.Marshal(tx)
}
