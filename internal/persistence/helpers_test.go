package persistence

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeStore records InsertBatch calls and can be told to fail the next N calls.
type fakeStore struct {
	mu       sync.Mutex
	batches  [][]Message
	calls    int
	failNext int
}

func (s *fakeStore) Migrate(context.Context) error { return nil }

func (s *fakeStore) InsertBatch(_ context.Context, msgs []Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.failNext > 0 {
		s.failNext--
		return errors.New("insert failed")
	}
	cp := make([]Message, len(msgs))
	copy(cp, msgs)
	s.batches = append(s.batches, cp)
	return nil
}

func (s *fakeStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, b := range s.batches {
		n += len(b)
	}
	return n
}

func (s *fakeStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *fakeStore) batchCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.batches)
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}
