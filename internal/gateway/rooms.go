package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
)

// ErrRoomExists is returned by Create when the id is already taken.
var ErrRoomExists = errors.New("room already exists")

// RoomRecord is a registered room as the gateway consumes it.
type RoomRecord struct {
	ID          string
	Visibility  string
	InviteToken string
}

// RoomStore is the room metadata the gateway depends on; an adapter over the
// persistence layer satisfies it, tests use a fake.
type RoomStore interface {
	Lookup(ctx context.Context, id string) (RoomRecord, bool, error)
	Create(ctx context.Context, r RoomRecord) error
	List(ctx context.Context) ([]RoomRecord, error)
	Delete(ctx context.Context, id string) error
}

func newInviteToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
