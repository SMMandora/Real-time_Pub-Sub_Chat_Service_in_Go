package gateway

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// heartbeatInterval is how often a connection refreshes its presence score.
	heartbeatInterval = 10 * time.Second
	// presenceTTLms is the window (ms) within which a member counts as present.
	presenceTTLms int64 = 30000
)

// PresenceStore tracks per-room membership with last-heartbeat scores.
type PresenceStore interface {
	Add(ctx context.Context, room, member string, scoreMs int64) error
	Remove(ctx context.Context, room, member string) error
	Snapshot(ctx context.Context, room string, minScoreMs int64) ([]string, error)
}

func presenceKey(room string) string { return "presence:" + room }

// RedisPresenceStore implements PresenceStore over a Redis sorted set per room.
type RedisPresenceStore struct {
	rdb *redis.Client
}

func NewRedisPresenceStore(addr string) *RedisPresenceStore {
	return &RedisPresenceStore{rdb: redis.NewClient(&redis.Options{Addr: addr})}
}

func (p *RedisPresenceStore) Add(ctx context.Context, room, member string, scoreMs int64) error {
	return p.rdb.ZAdd(ctx, presenceKey(room), redis.Z{Score: float64(scoreMs), Member: member}).Err()
}

func (p *RedisPresenceStore) Remove(ctx context.Context, room, member string) error {
	return p.rdb.ZRem(ctx, presenceKey(room), member).Err()
}

func (p *RedisPresenceStore) Snapshot(ctx context.Context, room string, minScoreMs int64) ([]string, error) {
	return p.rdb.ZRangeByScore(ctx, presenceKey(room), &redis.ZRangeBy{
		Min: strconv.FormatInt(minScoreMs, 10),
		Max: "+inf",
	}).Result()
}

func (p *RedisPresenceStore) Close() error { return p.rdb.Close() }
