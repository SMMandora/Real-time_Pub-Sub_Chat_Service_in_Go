package persistence

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestBatcherFlushesOnSize(t *testing.T) {
	store := &fakeStore{}
	b := NewBatcher(store, 3, time.Hour, testLogger())
	go b.Run()
	defer b.Close()

	for i := 0; i < 3; i++ {
		b.Submit(Message{RoomID: "x", ID: int64(i + 1), Body: "hi"})
	}
	waitUntil(t, time.Second, func() bool { return store.batchCount() == 1 && store.count() == 3 })
}

func TestBatcherFlushesOnInterval(t *testing.T) {
	store := &fakeStore{}
	b := NewBatcher(store, 1000, 20*time.Millisecond, testLogger())
	go b.Run()
	defer b.Close()

	b.Submit(Message{RoomID: "x", ID: 1, Body: "a"})
	b.Submit(Message{RoomID: "x", ID: 2, Body: "b"})
	waitUntil(t, time.Second, func() bool { return store.count() == 2 })
}

func TestBatcherFlushesRemainderOnClose(t *testing.T) {
	store := &fakeStore{}
	b := NewBatcher(store, 1000, time.Hour, testLogger())
	go b.Run()

	b.Submit(Message{RoomID: "x", ID: 1, Body: "a"})
	b.Submit(Message{RoomID: "x", ID: 2, Body: "b"})
	b.Close() // must flush the remainder

	if store.count() != 2 {
		t.Fatalf("expected 2 messages flushed on close, got %d", store.count())
	}
}

func TestBatcherRetriesOnceOnFailure(t *testing.T) {
	store := &fakeStore{failNext: 1}
	b := NewBatcher(store, 1, time.Hour, testLogger())
	go b.Run()
	defer b.Close()

	b.Submit(Message{RoomID: "x", ID: 1, Body: "a"})
	waitUntil(t, time.Second, func() bool { return store.callCount() == 2 && store.count() == 1 })
}
