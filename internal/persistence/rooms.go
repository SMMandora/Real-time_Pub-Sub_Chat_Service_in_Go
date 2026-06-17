package persistence

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// ErrRoomExists is returned by CreateRoom when the id is already taken.
var ErrRoomExists = errors.New("room already exists")

// RoomRecord is a registered room.
type RoomRecord struct {
	ID          string
	Name        string
	Description string
	Visibility  string
	InviteToken string
	CreatedMS   int64
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func (s *PgxStore) CreateRoom(ctx context.Context, r RoomRecord) error {
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO rooms (id, name, description, visibility, invite_token, created_ms)
		 VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (id) DO NOTHING`,
		r.ID, r.Name, r.Description, r.Visibility, nullIfEmpty(r.InviteToken), r.CreatedMS)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrRoomExists
	}
	return nil
}

func (s *PgxStore) GetRoom(ctx context.Context, id string) (RoomRecord, bool, error) {
	var r RoomRecord
	var token *string
	err := s.pool.QueryRow(ctx,
		`SELECT id, name, description, visibility, invite_token, created_ms FROM rooms WHERE id=$1`, id).
		Scan(&r.ID, &r.Name, &r.Description, &r.Visibility, &token, &r.CreatedMS)
	if errors.Is(err, pgx.ErrNoRows) {
		return RoomRecord{}, false, nil
	}
	if err != nil {
		return RoomRecord{}, false, err
	}
	if token != nil {
		r.InviteToken = *token
	}
	return r, true, nil
}

func (s *PgxStore) ListRooms(ctx context.Context) ([]RoomRecord, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, name, description, visibility, invite_token, created_ms FROM rooms ORDER BY id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RoomRecord
	for rows.Next() {
		var r RoomRecord
		var token *string
		if err := rows.Scan(&r.ID, &r.Name, &r.Description, &r.Visibility, &token, &r.CreatedMS); err != nil {
			return nil, err
		}
		if token != nil {
			r.InviteToken = *token
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *PgxStore) DeleteRoom(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM rooms WHERE id=$1`, id)
	return err
}
