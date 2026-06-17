package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestRedisRateLimiterBurstThenBlock(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	// Small capacity and negligible refill make the burst deterministic.
	rl := NewRedisRateLimiter(mr.Addr(), 5, 0.0001)
	defer rl.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		ok, err := rl.Allow(ctx, "alice")
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("request %d should be allowed", i+1)
		}
	}
	ok, err := rl.Allow(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("6th request should be blocked")
	}

	// A different user has an independent bucket.
	ok, err = rl.Allow(ctx, "bob")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("bob's first request should be allowed")
	}
}

func TestRedisRateLimiterRefillsOverTime(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	// Capacity 2, slow refill (0.5/s): fast back-to-back calls never refill a
	// whole token, but a >2s wait does.
	rl := NewRedisRateLimiter(mr.Addr(), 2, 0.5)
	defer rl.Close()
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		ok, err := rl.Allow(ctx, "alice")
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("request %d should be allowed", i+1)
		}
	}
	ok, err := rl.Allow(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("3rd request should be blocked before refill")
	}

	time.Sleep(2100 * time.Millisecond) // refills ~1 token at 0.5/s

	ok, err = rl.Allow(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("request should be allowed after refill")
	}
}
