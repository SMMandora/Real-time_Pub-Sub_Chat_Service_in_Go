package gateway

import (
	"context"
	"sync"
)

// fakeBus is an in-memory loopback Bus for tests: a Publish to a subscribed
// channel is delivered synchronously to the handler, simulating this gateway's
// own subscription receiving the message. It records calls for assertions.
type fakeBus struct {
	mu         sync.Mutex
	handler    func(channel string, payload []byte)
	subscribed map[string]bool
	published  []busMsg
	subCount   int
	pingErr    error
}

type busMsg struct {
	channel string
	payload []byte
}

func newFakeBus() *fakeBus {
	return &fakeBus{subscribed: make(map[string]bool)}
}

func (b *fakeBus) Publish(_ context.Context, channel string, payload []byte) error {
	b.mu.Lock()
	b.published = append(b.published, busMsg{channel: channel, payload: payload})
	h := b.handler
	deliver := b.subscribed[channel]
	b.mu.Unlock()
	if deliver && h != nil {
		h(channel, payload)
	}
	return nil
}

func (b *fakeBus) Subscribe(_ context.Context, channel string) error {
	b.mu.Lock()
	b.subscribed[channel] = true
	b.subCount++
	b.mu.Unlock()
	return nil
}

func (b *fakeBus) Unsubscribe(_ context.Context, channel string) error {
	b.mu.Lock()
	delete(b.subscribed, channel)
	b.mu.Unlock()
	return nil
}

func (b *fakeBus) SetHandler(h func(channel string, payload []byte)) {
	b.mu.Lock()
	b.handler = h
	b.mu.Unlock()
}

func (b *fakeBus) Ping(_ context.Context) error { return b.pingErr }
func (b *fakeBus) Close() error                 { return nil }

func (b *fakeBus) isSubscribed(channel string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.subscribed[channel]
}

func (b *fakeBus) publishCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.published)
}

func (b *fakeBus) subscribeCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.subCount
}
