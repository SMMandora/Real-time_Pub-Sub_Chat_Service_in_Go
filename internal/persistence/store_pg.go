package persistence

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgxStore persists messages to Postgres via a pgx connection pool.
type PgxStore struct {
	pool *pgxpool.Pool
}

func NewPgxStore(pool *pgxpool.Pool) *PgxStore { return &PgxStore{pool: pool} }

const createMessagesTable = `
CREATE TABLE IF NOT EXISTS messages (
	room_id    TEXT   NOT NULL,
	id         BIGINT NOT NULL,
	sender     TEXT   NOT NULL,
	body       TEXT   NOT NULL,
	created_ms BIGINT NOT NULL,
	PRIMARY KEY (room_id, id)
);`

func (s *PgxStore) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, createMessagesTable)
	return err
}

// InsertBatch writes all messages in a single transaction, ignoring rows whose
// (room_id, id) already exists.
func (s *PgxStore) InsertBatch(ctx context.Context, msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	batch := &pgx.Batch{}
	for _, m := range msgs {
		batch.Queue(
			`INSERT INTO messages (room_id, id, sender, body, created_ms)
			 VALUES ($1, $2, $3, $4, $5)
			 ON CONFLICT (room_id, id) DO NOTHING`,
			m.RoomID, m.ID, m.Sender, m.Body, m.CreatedMS,
		)
	}
	br := tx.SendBatch(ctx, batch)
	if err := br.Close(); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
