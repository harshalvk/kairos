package chaos

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// setupRedis starts a real Redis container for the duration of the
// test, torn down via t.Cleanup.
func setupRedis(t *testing.T) (*redis.Client, *tcredis.RedisContainer) {
	t.Helper()
	ctx := context.Background()

	container, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, container.Terminate(ctx))
	})

	connStr, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	opts, err := redis.ParseURL(connStr)
	require.NoError(t, err)

	rdb := redis.NewClient(opts)
	t.Cleanup(func() {
		if closeErr := rdb.Close(); closeErr != nil {
			t.Logf("failed to close redis client: %v", closeErr)
		}
	})

	return rdb, container
}
