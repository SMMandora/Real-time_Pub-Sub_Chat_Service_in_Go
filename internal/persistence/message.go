package persistence

import "context"

// Message is one persisted chat message. ID is the per-room sequence assigned
// by the gateway.
type Message struct {
	RoomID    string
	ID        int64
	Sender    string
	Body      string
	CreatedMS int64
}

// MessageStore persists batches of messages.
type MessageStore interface {
	Migrate(ctx context.Context) error
	InsertBatch(ctx context.Context, msgs []Message) error
}
