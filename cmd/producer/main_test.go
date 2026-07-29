package main

import (
	"context"
	"testing"
	"time"
)

func TestProduceLoop_RespectsCount(t *testing.T) {
	calls := 0
	produceLoop(context.Background(), 1000, 5, func() { calls++ })
	if calls != 5 {
		t.Fatalf("expected 5 calls, got %d", calls)
	}
}

func TestProduceLoop_StopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	done := make(chan struct{})

	go func() {
		produceLoop(ctx, 1000, 0, func() { calls++ }) // count=0 means run forever
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("produceLoop did not stop after context cancellation")
	}

	if calls == 0 {
		t.Fatal("expected at least one call before cancellation")
	}
}

func TestProduceLoop_PacesAtGivenRate(t *testing.T) {
	start := time.Now()
	calls := 0
	// rate=10/sec, count=3 -> ticks land around 100ms, 200ms, 300ms
	produceLoop(context.Background(), 10, 3, func() { calls++ })
	elapsed := time.Since(start)

	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
	if elapsed < 250*time.Millisecond {
		t.Fatalf("expected pacing to take at least ~250ms for 3 events at rate 10, took %v", elapsed)
	}
}

func TestProduceLoop_ZeroCallsWhenCanceledImmediately(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled before the loop starts

	calls := 0
	produceLoop(ctx, 1000, 5, func() { calls++ })

	if calls != 0 {
		t.Fatalf("expected 0 calls on a pre-canceled context, got %d", calls)
	}
}
