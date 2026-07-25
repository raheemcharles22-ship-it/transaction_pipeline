package event

import (
	"encoding/json"
	"math/rand/v2"
	"testing"
)

func TestGenerateTransaction_NoChaosUsesStableIdempotencyKey(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 1))
	merchants := []string{"m1", "m2"}
	var seenKeys []string

	tx := GenerateTransaction(r, merchants, false, 0.0, &seenKeys)

	if tx.ID == "" {
		t.Fatal("expected a non-empty ID")
	}
	if tx.IdempotencyKey != tx.ID {
		t.Fatalf("expected the idempotency key to match the ID when chaos is disabled, got id=%q key=%q", tx.ID, tx.IdempotencyKey)
	}
	if tx.MerchantID == "" {
		t.Fatal("expected a merchant ID to be set")
	}
	if tx.AmountCents <= 0 {
		t.Fatalf("expected a positive amount, got %d", tx.AmountCents)
	}
	if len(seenKeys) != 1 || seenKeys[0] != tx.ID {
		t.Fatalf("expected the new idempotency key to be tracked, got seenKeys=%v", seenKeys)
	}
}

func TestGenerateTransaction_ChaosCanReuseAnExistingIdempotencyKey(t *testing.T) {
	r := rand.New(rand.NewPCG(2, 2))
	merchants := []string{"m1"}
	seenKeys := []string{"existing-key"}

	tx := GenerateTransaction(r, merchants, true, 1.0, &seenKeys)

	if tx.IdempotencyKey != "existing-key" {
		t.Fatalf("expected chaos mode to reuse an existing idempotency key, got %q", tx.IdempotencyKey)
	}
	if len(seenKeys) != 1 {
		t.Fatalf("expected chaos reuse to avoid appending a new key, got seenKeys=%v", seenKeys)
	}
}

func TestGenerateTransaction_ChaosRateZeroNeverReusesExistingKeys(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 3))
	merchants := []string{"m1"}
	seenKeys := []string{"existing-key"}

	tx := GenerateTransaction(r, merchants, true, 0.0, &seenKeys)

	if tx.IdempotencyKey == "existing-key" {
		t.Fatal("expected a new idempotency key when chaos rate is zero")
	}
	if len(seenKeys) != 2 || seenKeys[1] != tx.IdempotencyKey {
		t.Fatalf("expected the new idempotency key to be appended, got seenKeys=%v", seenKeys)
	}
}

func TestGenerateTransaction_NilDependenciesUsesSafeDefaults(t *testing.T) {
	tx := GenerateTransaction(nil, nil, false, 0.0, nil)

	if tx.ID == "" {
		t.Fatal("expected a non-empty ID")
	}
	if tx.IdempotencyKey != tx.ID {
		t.Fatalf("expected idempotency key to match the generated ID, got id=%q key=%q", tx.ID, tx.IdempotencyKey)
	}
	if tx.MerchantID == "" {
		t.Fatal("expected a merchant ID to be set")
	}
}

func TestEncodePayload_NoChaosProducesCompletePayload(t *testing.T) {
	r := rand.New(rand.NewPCG(4, 4))
	merchants := []string{"m1"}
	var seenKeys []string
	tx := GenerateTransaction(r, merchants, false, 0.0, &seenKeys)

	payload, err := EncodePayload(tx, r, false, 0.0)
	if err != nil {
		t.Fatalf("unexpected error encoding payload: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("expected valid JSON, got error: %v", err)
	}

	for _, field := range []string{"id", "merchant_id", "amount_cents", "currency", "timestamp", "idempotency_key"} {
		if _, ok := decoded[field]; !ok {
			t.Fatalf("expected field %q to be present in a non-chaos payload", field)
		}
	}
}

func TestEncodePayload_ChaosDropsRequiredFields(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 5))
	merchants := []string{"m1"}
	var seenKeys []string
	tx := GenerateTransaction(r, merchants, false, 0.0, &seenKeys)

	payload, err := EncodePayload(tx, r, true, 1.0)
	if err != nil {
		t.Fatalf("unexpected error encoding payload: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("expected valid JSON, got error: %v", err)
	}

	if _, ok := decoded["merchant_id"]; ok {
		t.Fatal("expected chaos mode to omit merchant_id from the payload")
	}
	if _, ok := decoded["idempotency_key"]; ok {
		t.Fatal("expected chaos mode to omit idempotency_key from the payload")
	}
}

func TestEncodePayload_ChaosRateZeroKeepsFields(t *testing.T) {
	r := rand.New(rand.NewPCG(6, 6))
	merchants := []string{"m1"}
	var seenKeys []string
	tx := GenerateTransaction(r, merchants, false, 0.0, &seenKeys)

	payload, err := EncodePayload(tx, r, true, 0.0)
	if err != nil {
		t.Fatalf("unexpected error encoding payload: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("expected valid JSON, got error: %v", err)
	}

	for _, field := range []string{"merchant_id", "idempotency_key"} {
		if _, ok := decoded[field]; !ok {
			t.Fatalf("expected field %q to be present when chaos rate is zero", field)
		}
	}
}
