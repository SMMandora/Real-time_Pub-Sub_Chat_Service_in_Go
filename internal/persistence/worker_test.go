package persistence

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestWorkerHandleDecodesMessage(t *testing.T) {
	store := &fakeStore{}
	b := NewBatcher(store, 1, time.Hour, testLogger()) // size 1 flushes immediately
	go b.Run()
	defer b.Close()

	w := NewWorker(nil, b, testLogger())
	w.handle(`{"type":"message","room":"x","id":7,"from":"u1","text":"hi","ts":123}`)

	waitUntil(t, time.Second, func() bool { return store.count() == 1 })
	got := store.batches[0][0]
	if got.RoomID != "x" || got.ID != 7 || got.Sender != "u1" || got.Body != "hi" || got.CreatedMS != 123 {
		t.Fatalf("unexpected message: %+v", got)
	}
}

func TestWorkerHandleIgnoresNonMessageAndGarbage(t *testing.T) {
	store := &fakeStore{}
	b := NewBatcher(store, 1, time.Hour, testLogger())
	go b.Run()
	defer b.Close()

	w := NewWorker(nil, b, testLogger())
	w.handle(`{"type":"system","room":"x","event":"join","from":"u1"}`)
	w.handle(`not json`)

	time.Sleep(50 * time.Millisecond)
	if store.count() != 0 {
		t.Fatalf("expected nothing stored, got %d", store.count())
	}
}

func TestWorkerConsumesPublishedMessage(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	store := &fakeStore{}
	b := NewBatcher(store, 1, 20*time.Millisecond, testLogger())
	go b.Run()
	defer b.Close()

	w := NewWorker(rdb, b, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	// Republish until delivered: pub/sub may drop a publish issued before the
	// PSUBSCRIBE is active, so retry until the worker has stored the message.
	payload := `{"type":"message","room":"x","id":9,"from":"u2","text":"yo","ts":5}`
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rdb.Publish(context.Background(), "room:x", payload)
		if store.count() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if store.count() == 0 {
		t.Fatal("worker did not persist the published message")
	}
	got := store.batches[0][0]
	if got.RoomID != "x" || got.ID != 9 || got.Body != "yo" {
		t.Fatalf("unexpected message: %+v", got)
	}
}
