package gateway

import (
	"context"
	"sync"
)

type fakeHistory struct {
	mu        sync.Mutex
	recent    []StoredMessage
	since     []StoredMessage
	before    []StoredMessage
	err       error
	pingErr   error
	sinceArg  int64
	beforeArg int64
}

func (h *fakeHistory) Recent(_ context.Context, _ string, _ int) ([]StoredMessage, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.recent, h.err
}

func (h *fakeHistory) Since(_ context.Context, _ string, sinceID int64, _ int) ([]StoredMessage, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sinceArg = sinceID
	return h.since, h.err
}

func (h *fakeHistory) Before(_ context.Context, _ string, beforeID int64, _ int) ([]StoredMessage, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.beforeArg = beforeID
	return h.before, h.err
}

func (h *fakeHistory) Ping(_ context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.pingErr
}

func (h *fakeHistory) sinceCalledWith() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sinceArg
}

func (h *fakeHistory) beforeCalledWith() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.beforeArg
}
