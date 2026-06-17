package persistence

import (
	"context"
	"log/slog"
	"time"
)

// Batcher accumulates messages and flushes them to a MessageStore when the
// batch reaches maxSize or the flush interval elapses, whichever comes first.
type Batcher struct {
	store    MessageStore
	maxSize  int
	interval time.Duration
	log      *slog.Logger
	in       chan Message
	done     chan struct{}
	closed   chan struct{}
}

func NewBatcher(store MessageStore, maxSize int, interval time.Duration, log *slog.Logger) *Batcher {
	return &Batcher{
		store:    store,
		maxSize:  maxSize,
		interval: interval,
		log:      log,
		in:       make(chan Message, maxSize*2),
		done:     make(chan struct{}),
		closed:   make(chan struct{}),
	}
}

// Submit enqueues a message for batched persistence. Safe to call until Close.
func (b *Batcher) Submit(m Message) { b.in <- m }

// Run flushes batches until Close is called. Call it in its own goroutine.
func (b *Batcher) Run() {
	defer close(b.closed)
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()
	batch := make([]Message, 0, b.maxSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		b.writeBatch(batch)
		batch = batch[:0]
	}

	for {
		select {
		case m := <-b.in:
			QueueDepth.Set(float64(len(b.in)))
			batch = append(batch, m)
			if len(batch) >= b.maxSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-b.done:
			// Drain anything still queued, then do a final flush.
			for {
				select {
				case m := <-b.in:
					batch = append(batch, m)
					if len(batch) >= b.maxSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

// writeBatch persists a copy of the batch, retrying once before dropping.
func (b *Batcher) writeBatch(batch []Message) {
	msgs := make([]Message, len(batch))
	copy(msgs, batch)
	if err := b.tryInsert(msgs); err != nil {
		if err2 := b.tryInsert(msgs); err2 != nil {
			b.log.Warn("dropping batch after retry", "count", len(msgs), "err", err2)
			return
		}
	}
	MessagesPersisted.Add(float64(len(msgs)))
	BatchSize.Observe(float64(len(msgs)))
}

func (b *Batcher) tryInsert(msgs []Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return b.store.InsertBatch(ctx, msgs)
}

// Close stops Run and waits for the final flush to complete.
func (b *Batcher) Close() {
	close(b.done)
	<-b.closed
}
