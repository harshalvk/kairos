package leaderelection_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/harshalvk/kairos/internal/leaderelection"
)

func setupRedis(t *testing.T) *redis.Client {
	t.Helper()
	ctx := context.Background()

	container, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.Terminate(ctx)) })

	connStr, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	opts, err := redis.ParseURL(connStr)
	require.NoError(t, err)

	return redis.NewClient(opts)
}

func TestOnlyOneOfTwoNodesAcquiresLeadership(t *testing.T) {
	rdb := setupRedis(t)
	ctx := context.Background()
	logger := slog.Default()

	nodeA := leaderelection.New(rdb, "node-a", 10*time.Second, logger)
	nodeB := leaderelection.New(rdb, "node-b", 10*time.Second, logger)

	aAcquired := nodeA.TryAcquire(ctx)
	bAcquired := nodeB.TryAcquire(ctx)

	assert.True(t, aAcquired)
	assert.False(t, bAcquired)
	assert.True(t, nodeA.IsLeader())
	assert.False(t, nodeB.IsLeader())
}

func TestReleaseAllowsAnotherNodeToAcquire(t *testing.T) {
	rdb := setupRedis(t)
	ctx := context.Background()
	logger := slog.Default()

	nodeA := leaderelection.New(rdb, "node-a", 10*time.Second, logger)
	nodeB := leaderelection.New(rdb, "node-b", 10*time.Second, logger)

	require.True(t, nodeA.TryAcquire(ctx))
	require.NoError(t, nodeA.Release(ctx))

	assert.True(t, nodeB.TryAcquire(ctx))
}

func TestRenewFailsIfAnotherNodeHoldsTheLock(t *testing.T) {
	rdb := setupRedis(t)
	ctx := context.Background()
	logger := slog.Default()

	nodeA := leaderelection.New(rdb, "node-a", 1*time.Second, logger)
	nodeB := leaderelection.New(rdb, "node-b", 10*time.Second, logger)

	require.True(t, nodeA.TryAcquire(ctx))
	time.Sleep(1100 * time.Millisecond) // let nodeA's lock expire

	require.True(t, nodeB.TryAcquire(ctx)) // nodeB takes over

	err := nodeA.Renew(ctx) // nodeA tries to renew a lock it no longer holds
	assert.Error(t, err)
}

func TestTTLExpiryAllowsFailover(t *testing.T) {
	rdb := setupRedis(t)
	ctx := context.Background()
	logger := slog.Default()

	nodeA := leaderelection.New(rdb, "node-a", 500*time.Millisecond, logger)
	nodeB := leaderelection.New(rdb, "node-b", 500*time.Millisecond, logger)

	require.True(t, nodeA.TryAcquire(ctx))
	assert.False(t, nodeB.TryAcquire(ctx)) // still held

	time.Sleep(600 * time.Millisecond) // let it expire, no renewal

	assert.True(t, nodeB.TryAcquire(ctx)) // now available
}
