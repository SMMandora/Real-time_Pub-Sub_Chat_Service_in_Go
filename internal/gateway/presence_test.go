package gateway

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

func TestRedisPresenceAddSnapshotRemove(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	ps := NewRedisPresenceStore(mr.Addr())
	defer ps.Close()
	ctx := context.Background()

	if err := ps.Add(ctx, "x", "a", 1000); err != nil {
		t.Fatal(err)
	}
	if err := ps.Add(ctx, "x", "b", 2000); err != nil {
		t.Fatal(err)
	}

	got, err := ps.Snapshot(ctx, "x", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 members, got %v", got)
	}

	// minScore filter excludes the stale member (score 1000 < 1500).
	got, err = ps.Snapshot(ctx, "x", 1500)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "b" {
		t.Fatalf("expected [b], got %v", got)
	}

	if err := ps.Remove(ctx, "x", "b"); err != nil {
		t.Fatal(err)
	}
	if err := ps.Remove(ctx, "x", "a"); err != nil {
		t.Fatal(err)
	}
	got, _ = ps.Snapshot(ctx, "x", 0)
	if len(got) != 0 {
		t.Fatalf("expected empty after remove, got %v", got)
	}
}
