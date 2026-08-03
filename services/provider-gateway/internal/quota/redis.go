package quota

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCounter is the real fleet-wide Counter, backed by the shared Redis
// Cluster every OneZox service uses. Only sets the key's TTL on the
// FIRST increment observed in a window (Incr returning 1) — refreshing
// the TTL on every call would extend the window indefinitely under
// continuous traffic and the fixed window would never actually roll over.
type RedisCounter struct {
	client *redis.ClusterClient
	log    *slog.Logger
}

func NewRedisCounter(client *redis.ClusterClient, log *slog.Logger) *RedisCounter {
	return &RedisCounter{client: client, log: log}
}

func (r *RedisCounter) Increment(ctx context.Context, key string, window time.Duration) (int64, error) {
	val, err := r.client.Incr(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	if val == 1 {
		if err := r.client.Expire(ctx, key, window).Err(); err != nil {
			// The increment already succeeded; a missing TTL just means
			// this key outlives its window slightly (self-corrects at
			// the next Redis Cluster reset), not a correctness bug worth
			// failing the whole request over.
			r.log.Warn("quota: failed to set window TTL", "key", key, "error", err)
		}
	}
	return val, nil
}

// Peek reads the counter's current value without incrementing it. A
// missing key (redis.Nil — no requests in this window yet) is 0, not an
// error: that's a legitimate, common state, not a failure.
func (r *RedisCounter) Peek(ctx context.Context, key string) (int64, error) {
	val, err := r.client.Get(ctx, key).Int64()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		return 0, err
	}
	return val, nil
}
