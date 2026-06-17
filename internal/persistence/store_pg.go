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

const createRoomsTable = `
CREATE TABLE IF NOT EXISTS rooms (
	id           TEXT PRIMARY KEY,
	visibility   TEXT NOT NULL,
	invite_token TEXT,
	created_ms   BIGINT NOT NULL
);`

func (s *PgxStore) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, createMessagesTable); err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx, createRoomsTable); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx,
		`ALTER TABLE rooms ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT '';
		 ALTER TABLE rooms ADD COLUMN IF NOT EXISTS description TEXT NOT NULL DEFAULT '';`)
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

func (s *PgxStore) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

const selectColumns = `SELECT room_id, id, sender, body, created_ms FROM messages`

func (s *PgxStore) RecentMessages(ctx context.Context, room string, limit int) ([]Message, error) {
	rows, err := s.pool.Query(ctx,
		selectColumns+` WHERE room_id=$1 ORDER BY id DESC LIMIT $2`, room, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	msgs, err := scanMessages(rows)
	if err != nil {
		return nil, err
	}
	reverse(msgs)
	return msgs, nil
}

func (s *PgxStore) MessagesSince(ctx context.Context, room string, sinceID int64, limit int) ([]Message, error) {
	rows, err := s.pool.Query(ctx,
		selectColumns+` WHERE room_id=$1 AND id > $2 ORDER BY id ASC LIMIT $3`, room, sinceID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMessages(rows)
}

func (s *PgxStore) MessagesBefore(ctx context.Context, room string, beforeID int64, limit int) ([]Message, error) {
	rows, err := s.pool.Query(ctx,
		selectColumns+` WHERE room_id=$1 AND id < $2 ORDER BY id DESC LIMIT $3`, room, beforeID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	msgs, err := scanMessages(rows)
	if err != nil {
		return nil, err
	}
	reverse(msgs)
	return msgs, nil
}

func scanMessages(rows pgx.Rows) ([]Message, error) {
	var out []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.RoomID, &m.ID, &m.Sender, &m.Body, &m.CreatedMS); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func reverse(msgs []Message) {
	for i, j := 0, len(msgs)-1; i < j; i, j = i+1, j-1 {
		msgs[i], msgs[j] = msgs[j], msgs[i]
	}
}
