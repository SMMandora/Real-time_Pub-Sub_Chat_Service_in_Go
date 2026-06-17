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

type MemberRecord struct {
	Username   string
	LastSeenMs int64
}

func (s *PgxStore) TouchMember(ctx context.Context, room, username string, lastSeenMs int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO room_members (room_id, username, last_seen_ms)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (room_id, username) DO UPDATE SET last_seen_ms = EXCLUDED.last_seen_ms`,
		room, username, lastSeenMs)
	return err
}

func (s *PgxStore) ListMembers(ctx context.Context, room string) ([]MemberRecord, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT username, last_seen_ms FROM room_members WHERE room_id=$1 ORDER BY username ASC`, room)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MemberRecord
	for rows.Next() {
		var m MemberRecord
		if err := rows.Scan(&m.Username, &m.LastSeenMs); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
