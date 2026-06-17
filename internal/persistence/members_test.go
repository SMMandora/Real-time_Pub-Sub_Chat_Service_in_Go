package persistence

import (
	"context"
	"testing"
)

func TestMemberStoreTouchAndList(t *testing.T) {
	pool := startPostgres(t)
	ctx := context.Background()
	store := NewPgxStore(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.TouchMember(ctx, "x", "alice", 100); err != nil {
		t.Fatal(err)
	}
	if err := store.TouchMember(ctx, "x", "bob", 50); err != nil {
		t.Fatal(err)
	}
	if err := store.TouchMember(ctx, "x", "alice", 200); err != nil { // upsert
		t.Fatal(err)
	}
	got, err := store.ListMembers(ctx, "x")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Username != "alice" || got[0].LastSeenMs != 200 {
		t.Fatalf("unexpected members: %+v", got)
	}
}
