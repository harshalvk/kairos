package chaos

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/queue"
	"github.com/harshalvk/kairos/internal/tenant"
)

// reconnectAfterRestart re-resolves the container's connection string
// and returns a fresh client. Necessary on Docker Desktop for Windows,
// where container.Restart() does not reliably preserve the host-side
// port-forwarding proxy for the original client's address — the
// container comes back up on a port the original client never learns
// about, so it retries a now-dead port forever rather than actually
// being slow to recover.
func reconnectAfterRestart(ctx context.Context, t *testing.T, container *tcredis.RedisContainer) *redis.Client {
	t.Helper()

	var newRdb *redis.Client
	require.Eventually(t, func() bool {
		connStr, err := container.ConnectionString(ctx)
		if err != nil {
			return false
		}
		opts, err := redis.ParseURL(connStr)
		if err != nil {
			return false
		}
		candidate := redis.NewClient(opts)
		if pingErr := candidate.Ping(ctx).Err(); pingErr != nil {
			if closeErr := candidate.Close(); closeErr != nil {
				t.Logf("failed to close probe client: %v", closeErr)
			}
			return false
		}
		newRdb = candidate
		return true
	}, 30*time.Second, 500*time.Millisecond, "redis did not become reachable via a freshly-resolved connection string after restart")

	t.Cleanup(func() {
		if closeErr := newRdb.Close(); closeErr != nil {
			t.Logf("failed to close reconnected redis client: %v", closeErr)
		}
	})
	return newRdb
}

// TestChaos_DelayedJobSurvivesRedisRestart verifies the claim in ADR
// 0003: delayed/retry jobs are durable Redis state (a sorted set), not
// an in-memory timer, so they survive the Redis process restarting.
func TestChaos_DelayedJobSurvivesRedisRestart(t *testing.T) {
	ctx := context.Background()
	rdb, container := setupRedis(t)
	registry := tenant.NewRegistry(rdb)
	q := queue.New(rdb, registry)

	payload, err := json.Marshal(map[string]string{"to": "test@example.com"})
	require.NoError(t, err)
	j := job.New("send_email", payload, 3)

	require.NoError(t, q.EnqueueDelayed(ctx, j, time.Now().Add(-1*time.Second)))

	require.NoError(t, container.Stop(ctx, nil))
	require.NoError(t, container.Start(ctx))

	newRdb := reconnectAfterRestart(ctx, t, container)
	q = queue.New(newRdb, registry) // durable state lives in Redis itself, not the client — a fresh client against the same data proves that

	promoted, err := q.PromoteDueJobs(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, promoted, "delayed job should have survived the Redis restart and still be promotable")

	got, err := q.Dequeue(ctx, []string{"send_email"}, 2*time.Second)
	require.NoError(t, err)
	assert.Equal(t, j.ID, got.ID)
}

// TestChaos_PendingJobSurvivesRedisRestart is the same verification for
// a plain pending-queue job (not delayed).
func TestChaos_PendingJobSurvivesRedisRestart(t *testing.T) {
	ctx := context.Background()
	rdb, container := setupRedis(t)
	registry := tenant.NewRegistry(rdb)
	q := queue.New(rdb, registry)

	payload, err := json.Marshal(map[string]string{"to": "test@example.com"})
	require.NoError(t, err)
	j := job.New("send_email", payload, 3)

	require.NoError(t, q.Enqueue(ctx, j))

	require.NoError(t, container.Stop(ctx, nil))
	require.NoError(t, container.Start(ctx))

	newRdb := reconnectAfterRestart(ctx, t, container)
	q = queue.New(newRdb, registry)

	got, err := q.Dequeue(ctx, []string{"send_email"}, 2*time.Second)
	require.NoError(t, err)
	assert.Equal(t, j.ID, got.ID)
}
