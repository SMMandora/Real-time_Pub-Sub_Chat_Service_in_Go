package gateway

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RateLimiter decides whether a user may send another message now.
type RateLimiter interface {
	Allow(ctx context.Context, user string) (bool, error)
}

// tokenBucketScript atomically refills based on elapsed time and consumes one
// token. KEYS[1] = bucket key; ARGV = capacity, refillPerSec, nowMs, cost.
var tokenBucketScript = redis.NewScript(`
local cap = tonumber(ARGV[1])
local refill = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])
local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
  tokens = cap
  ts = now
end
local elapsed = (now - ts) / 1000.0
tokens = math.min(cap, tokens + elapsed * refill)
local allowed = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
end
-- tokens is a float; Redis round-trips it through a string faithfully.
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], 120000)
return allowed
`)

// RedisRateLimiter implements a per-user token bucket in Redis.
type RedisRateLimiter struct {
	rdb          *redis.Client
	capacity     int
	refillPerSec float64
}

func NewRedisRateLimiter(addr string, capacity int, refillPerSec float64) *RedisRateLimiter {
	return &RedisRateLimiter{
		rdb:          redis.NewClient(&redis.Options{Addr: addr}),
		capacity:     capacity,
		refillPerSec: refillPerSec,
	}
}

func rateLimitKey(user string) string { return "ratelimit:" + user }

func (r *RedisRateLimiter) Allow(ctx context.Context, user string) (bool, error) {
	// now is this gateway's wall clock. Under cross-gateway clock skew the
	// stored ts can jump backward, making elapsed negative and stalling the
	// refill for that call — never over-filling, so it fails safe (stricter).
	res, err := tokenBucketScript.Run(ctx, r.rdb, []string{rateLimitKey(user)},
		r.capacity, r.refillPerSec, time.Now().UnixMilli(), 1).Int64()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

func (r *RedisRateLimiter) Close() error { return r.rdb.Close() }
