package persistence

import (
	"context"
	"errors"
	"testing"
)

func TestRoomStoreCRUD(t *testing.T) {
	pool := startPostgres(t)
	ctx := context.Background()
	store := NewPgxStore(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	if err := store.CreateRoom(ctx, RoomRecord{ID: "team", Name: "Team", Description: "private team room", Visibility: "private", InviteToken: "tok", CreatedMS: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRoom(ctx, RoomRecord{ID: "team", Visibility: "private", InviteToken: "x", CreatedMS: 2}); !errors.Is(err, ErrRoomExists) {
		t.Fatalf("expected ErrRoomExists on duplicate, got %v", err)
	}

	got, found, err := store.GetRoom(ctx, "team")
	if err != nil || !found {
		t.Fatalf("get team: err=%v found=%v", err, found)
	}
	if got.Visibility != "private" || got.InviteToken != "tok" {
		t.Fatalf("unexpected room: %+v", got)
	}
	if got.Name != "Team" || got.Description != "private team room" {
		t.Fatalf("expected name/description round-trip, got %+v", got)
	}

	if err := store.CreateRoom(ctx, RoomRecord{ID: "lounge", Visibility: "public", CreatedMS: 3}); err != nil {
		t.Fatal(err)
	}
	pub, _, _ := store.GetRoom(ctx, "lounge")
	if pub.InviteToken != "" {
		t.Fatalf("public room should have empty token, got %q", pub.InviteToken)
	}

	rooms, err := store.ListRooms(ctx)
	if err != nil || len(rooms) != 2 {
		t.Fatalf("list: err=%v n=%d", err, len(rooms))
	}

	if _, found, _ := store.GetRoom(ctx, "ghost"); found {
		t.Fatal("ghost should not be found")
	}

	if err := store.DeleteRoom(ctx, "team"); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := store.GetRoom(ctx, "team"); found {
		t.Fatal("team should be deleted")
	}
}
