package gateway

import (
	"context"
	"strings"

	"github.com/redis/go-redis/v9"
)

const (
	roomChannelPrefix = "room:"
	// controlChannel keeps the pub/sub connection active even when no rooms
	// are subscribed yet, avoiding empty-subscription edge cases.
	controlChannel = "gateway:control"
)

func roomChannel(id string) string { return roomChannelPrefix + id }

func roomFromChannel(channel string) string {
	return strings.TrimPrefix(channel, roomChannelPrefix)
}

// Bus is the cross-gateway message transport. A payload published to a channel
// is delivered to every gateway subscribed to that channel.
type Bus interface {
	Publish(ctx context.Context, channel string, payload []byte) error
	Subscribe(ctx context.Context, channel string) error
	Unsubscribe(ctx context.Context, channel string) error
	SetHandler(func(channel string, payload []byte))
	Ping(ctx context.Context) error
	Close() error
}

// RedisBus implements Bus over a single Redis pub/sub connection. One receive
// goroutine ranges over incoming messages and invokes the handler.
type RedisBus struct {
	rdb    *redis.Client
	pubsub *redis.PubSub
}

// NewRedisBus connects to Redis at addr and subscribes to an internal control
// channel so the pub/sub connection is always active.
func NewRedisBus(addr string) *RedisBus {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	pubsub := rdb.Subscribe(context.Background(), controlChannel)
	return &RedisBus{rdb: rdb, pubsub: pubsub}
}

func (b *RedisBus) Publish(ctx context.Context, channel string, payload []byte) error {
	return b.rdb.Publish(ctx, channel, payload).Err()
}

func (b *RedisBus) Subscribe(ctx context.Context, channel string) error {
	return b.pubsub.Subscribe(ctx, channel)
}

func (b *RedisBus) Unsubscribe(ctx context.Context, channel string) error {
	return b.pubsub.Unsubscribe(ctx, channel)
}

// SetHandler registers the delivery callback and starts the receive goroutine.
// Call exactly once before messages are expected. The goroutine exits when the
// pub/sub connection is closed by Close.
func (b *RedisBus) SetHandler(handler func(channel string, payload []byte)) {
	go func() {
		for msg := range b.pubsub.Channel() {
			if msg.Channel == controlChannel {
				continue
			}
			handler(msg.Channel, []byte(msg.Payload))
		}
	}()
}

func (b *RedisBus) Ping(ctx context.Context) error {
	return b.rdb.Ping(ctx).Err()
}

func (b *RedisBus) Close() error {
	_ = b.pubsub.Close()
	return b.rdb.Close()
}
