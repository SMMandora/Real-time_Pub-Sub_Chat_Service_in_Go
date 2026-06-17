package persistence

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/redis/go-redis/v9"
)

const roomPattern = "room:*"

// inbound mirrors the gateway's message frame (only the fields we persist).
// Its json tags must match what the gateway publishes.
type inbound struct {
	Type string `json:"type"`
	Room string `json:"room"`
	ID   int64  `json:"id"`
	From string `json:"from"`
	Text string `json:"text"`
	TS   int64  `json:"ts"`
}

// Worker pattern-subscribes to room:* and forwards decoded chat messages to a
// Batcher.
type Worker struct {
	rdb     *redis.Client
	batcher *Batcher
	log     *slog.Logger
}

func NewWorker(rdb *redis.Client, batcher *Batcher, log *slog.Logger) *Worker {
	return &Worker{rdb: rdb, batcher: batcher, log: log}
}

// Run consumes messages until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	pubsub := w.rdb.PSubscribe(ctx, roomPattern)
	defer pubsub.Close()
	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			w.handle(msg.Payload)
		}
	}
}

// handle decodes a payload and, if it is a chat message, submits it to the
// batcher. Non-message or malformed payloads are skipped.
func (w *Worker) handle(payload string) {
	var in inbound
	if err := json.Unmarshal([]byte(payload), &in); err != nil {
		w.log.Debug("skipping malformed payload", "err", err)
		return
	}
	if in.Type != "message" {
		return
	}
	w.batcher.Submit(Message{
		RoomID:    in.Room,
		ID:        in.ID,
		Sender:    in.From,
		Body:      in.Text,
		CreatedMS: in.TS,
	})
}
