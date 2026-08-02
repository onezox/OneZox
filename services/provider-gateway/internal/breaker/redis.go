package breaker

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
)

// RedisStore is the real fleet-wide Store, backed by the shared Redis
// Cluster every OneZox service uses.
type RedisStore struct {
	client *redis.ClusterClient
}

func NewRedisStore(client *redis.ClusterClient) *RedisStore {
	return &RedisStore{client: client}
}

func (s *RedisStore) Get(ctx context.Context, key string) (*record, error) {
	data, err := s.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil // no record yet == Closed, per record.effectiveState
	}
	if err != nil {
		return nil, err
	}
	return unmarshal(data)
}

func (s *RedisStore) Set(ctx context.Context, key string, r *record) error {
	data, err := marshal(r)
	if err != nil {
		return err
	}
	return s.client.Set(ctx, key, data, 0).Err()
}
