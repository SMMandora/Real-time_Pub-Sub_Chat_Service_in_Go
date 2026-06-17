package persistence

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func startPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	container, err := postgres.Run(ctx, "postgres:16",
		postgres.WithDatabase("chat"),
		postgres.WithUsername("chat"),
		postgres.WithPassword("chat"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		t.Skipf("skipping: Docker/testcontainers unavailable: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestPgxStoreInsertAndDedup(t *testing.T) {
	pool := startPostgres(t)
	ctx := context.Background()
	store := NewPgxStore(pool)

	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	if err := store.InsertBatch(ctx, []Message{
		{RoomID: "x", ID: 1, Sender: "a", Body: "first", CreatedMS: 10},
		{RoomID: "x", ID: 2, Sender: "b", Body: "second", CreatedMS: 20},
	}); err != nil {
		t.Fatal(err)
	}

	// Duplicate (x,1) with a different body must be ignored (ON CONFLICT).
	if err := store.InsertBatch(ctx, []Message{
		{RoomID: "x", ID: 1, Sender: "a", Body: "CHANGED", CreatedMS: 99},
	}); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE room_id=$1`, "x").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("expected 2 rows, got %d", count)
	}

	var body string
	if err := pool.QueryRow(ctx, `SELECT body FROM messages WHERE room_id=$1 AND id=$2`, "x", 1).Scan(&body); err != nil {
		t.Fatal(err)
	}
	if body != "first" {
		t.Fatalf("expected original body preserved, got %q", body)
	}

	// Ordered retrieval by id.
	rows, err := pool.Query(ctx, `SELECT id FROM messages WHERE room_id=$1 ORDER BY id ASC`, "x")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("expected ids [1 2], got %v", ids)
	}
}
