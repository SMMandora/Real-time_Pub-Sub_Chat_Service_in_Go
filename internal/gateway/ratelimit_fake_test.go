package gateway

import (
	"context"
	"sync"
)

// fakeRateLimiter is mutex-guarded because a server's shared instance is used
// concurrently by multiple connection goroutines in the WebSocket e2e tests.
type fakeRateLimiter struct {
	mu    sync.Mutex
	allow bool
	err   error
	calls int
}

func (f *fakeRateLimiter) Allow(context.Context, string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.allow, f.err
}
