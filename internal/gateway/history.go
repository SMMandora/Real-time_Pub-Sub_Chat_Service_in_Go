package gateway

import "context"

// StoredMessage is a persisted message as the gateway consumes it for replay
// and history responses.
type StoredMessage struct {
	ID   int64  `json:"id"`
	From string `json:"from"`
	Text string `json:"text"`
	TS   int64  `json:"ts"`
}

// history is the read store the gateway depends on. An adapter over the
// persistence layer satisfies it; tests use a fake.
type history interface {
	Recent(ctx context.Context, room string, limit int) ([]StoredMessage, error)
	Since(ctx context.Context, room string, sinceID int64, limit int) ([]StoredMessage, error)
	Before(ctx context.Context, room string, beforeID int64, limit int) ([]StoredMessage, error)
	Ping(ctx context.Context) error
}
