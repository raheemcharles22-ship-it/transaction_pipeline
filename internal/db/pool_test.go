package db

import (
	"context"
	"testing"
)

func TestNewPool(t *testing.T) {
	ctx := context.Background()
	pool, err := NewPool(ctx, "postgres://pipeline:pipeline@localhost:5432/transactions?sslmode=disable")
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	defer pool.Close()
}
