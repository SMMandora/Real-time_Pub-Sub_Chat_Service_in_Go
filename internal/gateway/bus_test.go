package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestRedisBusPublishReachesSubscribedHandler(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	bus := NewRedisBus(mr.Addr())
	defer bus.Close()

	got := make(chan string, 1)
	bus.SetHandler(func(channel string, payload []byte) {
		got <- channel + "|" + string(payload)
	})
	if err := bus.Subscribe(context.Background(), "room:x"); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(context.Background(), "room:x", []byte("hello")); err != nil {
		t.Fatal(err)
	}

	select {
	case s := <-got:
		if s != "room:x|hello" {
			t.Fatalf("got %q, want room:x|hello", s)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not called within 2s")
	}
}

func TestRedisBusUnsubscribeStopsDelivery(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	bus := NewRedisBus(mr.Addr())
	defer bus.Close()

	got := make(chan string, 1)
	bus.SetHandler(func(channel string, payload []byte) { got <- string(payload) })

	if err := bus.Subscribe(context.Background(), "room:x"); err != nil {
		t.Fatal(err)
	}
	if err := bus.Unsubscribe(context.Background(), "room:x"); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(context.Background(), "room:x", []byte("hello")); err != nil {
		t.Fatal(err)
	}

	select {
	case s := <-got:
		t.Fatalf("expected no delivery after unsubscribe, got %q", s)
	case <-time.After(300 * time.Millisecond):
		// success: nothing delivered
	}
}

func TestRedisBusPing(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	bus := NewRedisBus(mr.Addr())
	defer bus.Close()

	if err := bus.Ping(context.Background()); err != nil {
		t.Fatalf("ping against live miniredis failed: %v", err)
	}
}

func TestRedisBusNextIDIncrementsPerRoom(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	bus := NewRedisBus(mr.Addr())
	defer bus.Close()
	ctx := context.Background()

	id1, err := bus.NextID(ctx, "x")
	if err != nil {
		t.Fatal(err)
	}
	id2, _ := bus.NextID(ctx, "x")
	idY, _ := bus.NextID(ctx, "y")

	if id1 != 1 || id2 != 2 {
		t.Fatalf("room x ids = %d,%d, want 1,2", id1, id2)
	}
	if idY != 1 {
		t.Fatalf("room y id = %d, want 1", idY)
	}
}
