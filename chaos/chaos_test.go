// Package chaos contains tests that deliberately kill infrastructure
// mid-run to verify Kairos's failure-recovery claims (durable retries,
// leader election failover, crash-safe delayed jobs) actually hold —
// not just that the code compiles and looks right.
package chaos

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/queue"
	"github.com/harshalvk/kairos/internal/tenant"
)

// TestChaos_DelayedJobSurvivesRedisRestart verifies the claim in ADR
// 0003: delayed/retry jobs are durable Redis state (a sorted set), not
// an in-memory timer, so they survive the Redis process restarting —
// e.g. an OOM-kill-and-recover or a rolling upgrade — not just a
// worker process restarting.
func TestChaos_DelayedJobSurvivesRedisRestart(t *testing.T) {
	ctx := context.Background()
	rdb, container := setupRedis(t)
	registry := tenant.NewRegistry(rdb)
	q := queue.New(rdb, registry)

	payload, err := json.Marshal(map[string]string{"to": "test@example.com"})
	require.NoError(t, err)
	j := job.New("send_email", payload, 3)

	// Schedule a job due in the past, so it's immediately promotable
	// once Redis is back.
	require.NoError(t, q.EnqueueDelayed(ctx, j, time.Now().Add(-1*time.Second)))

	// Simulate Redis's process restarting mid-flight. go-redis's client
	// is pooled and reconnects lazily on the next command against the
	// same address — no need to construct a fresh client, which is
	// itself part of what this test is verifying (the client and the
	// durable server-side state both recover without extra plumbing).
	require.NoError(t, container.Stop(ctx, nil))
	require.NoError(t, container.Start(ctx))

	require.Eventually(t, func() bool {
		return rdb.Ping(ctx).Err() == nil
	}, 15*time.Second, 200*time.Millisecond, "redis did not become reachable after restart")

	promoted, err := q.PromoteDueJobs(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, promoted, "delayed job should have survived the Redis restart and still be promotable")

	got, err := q.Dequeue(ctx, []string{"send_email"}, 2*time.Second)
	require.NoError(t, err)
	assert.Equal(t, j.ID, got.ID)
}

// TestChaos_PendingJobSurvivesRedisRestart is the same verification for
// a plain pending-queue job (not delayed) — confirming basic Enqueue
// state also survives a Redis process restart, as a baseline alongside
// the delayed-job case above.
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

	require.Eventually(t, func() bool {
		return rdb.Ping(ctx).Err() == nil
	}, 15*time.Second, 200*time.Millisecond, "redis did not become reachable after restart")

	got, err := q.Dequeue(ctx, []string{"send_email"}, 2*time.Second)
	require.NoError(t, err)
	assert.Equal(t, j.ID, got.ID)
}
