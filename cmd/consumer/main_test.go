package main

import "testing"

func TestClassifyTransaction_ValidPayload(t *testing.T) {
	raw := []byte(`{"id":"abc","merchant_id":"m1","amount_cents":100,"currency":"USD","timestamp":"2026-01-01T00:00:00Z","idempotency_key":"abc"}`)

	tx, reason, ok := classifyTransaction(raw)
	if !ok {
		t.Fatalf("expected valid payload to classify as ok, got reason=%q", reason)
	}
	if reason != "" {
		t.Fatalf("expected empty reason for a valid payload, got %q", reason)
	}
	if tx.MerchantID != "m1" {
		t.Fatalf("expected merchant_id m1, got %q", tx.MerchantID)
	}
	if tx.IdempotencyKey != "abc" {
		t.Fatalf("expected idempotency_key abc, got %q", tx.IdempotencyKey)
	}
}

func TestClassifyTransaction_MalformedJSON(t *testing.T) {
	raw := []byte(`{not valid json`)

	_, reason, ok := classifyTransaction(raw)
	if ok {
		t.Fatal("expected malformed JSON to be rejected")
	}
	if reason == "" {
		t.Fatal("expected a non-empty rejection reason for malformed JSON")
	}
}

func TestClassifyTransaction_MissingMerchantID(t *testing.T) {
	raw := []byte(`{"id":"abc","amount_cents":100,"currency":"USD","idempotency_key":"abc"}`)

	_, reason, ok := classifyTransaction(raw)
	if ok {
		t.Fatal("expected missing merchant_id to be rejected")
	}
	if reason != "missing required field" {
		t.Fatalf("expected reason %q, got %q", "missing required field", reason)
	}
}

func TestClassifyTransaction_MissingIdempotencyKey(t *testing.T) {
	raw := []byte(`{"id":"abc","merchant_id":"m1","amount_cents":100,"currency":"USD"}`)

	_, reason, ok := classifyTransaction(raw)
	if ok {
		t.Fatal("expected missing idempotency_key to be rejected")
	}
	if reason != "missing required field" {
		t.Fatalf("expected reason %q, got %q", "missing required field", reason)
	}
}

func TestClassifyTransaction_EmptyPayload(t *testing.T) {
	raw := []byte(`{}`)

	_, reason, ok := classifyTransaction(raw)
	if ok {
		t.Fatal("expected an empty object to be rejected")
	}
	if reason != "missing required field" {
		t.Fatalf("expected reason %q, got %q", "missing required field", reason)
	}
}
