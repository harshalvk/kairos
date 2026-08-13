package chaos

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/harshalvk/kairos/internal/leaderelection"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChaos_LeaderFailoverOnCrash(t *testing.T) {
	ctx := context.Background()
	rdb, _ := setupRedis(t) // container restart not needed for this test

	logger := slog.Default()
	nodeA := leaderelection.New(rdb, "node-a", 500*time.Millisecond, logger)
	nodeB := leaderelection.New(rdb, "node-b", 500*time.Millisecond, logger)

	require.True(t, nodeA.TryAcquire(ctx))
	assert.False(t, nodeB.TryAcquire(ctx))

	// simulate node-a crashing outright — no graceful Release, just stop
	// renewing. This tests failover via TTL expiry alone, with zero
	// cooperation from the dead node.

	require.Eventually(t, func() bool {
		return nodeB.TryAcquire(ctx)
	}, 2*time.Second, 100*time.Millisecond, "node-b should have acquired leadership after node-a's lock TTL expired without renewal")

	assert.True(t, nodeB.IsLeader())
}
