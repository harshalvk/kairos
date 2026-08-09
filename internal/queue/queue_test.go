package queue_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/queue"
	"github.com/harshalvk/kairos/internal/tenant"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// setupRedis starts a real Redis container for the duration of the test
// and returns a connected client. testContainers handles teardown via
// t.Cleanup, so test never leak containers even on failure
func setupRedis(t *testing.T) *redis.Client {
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

	return redis.NewClient(opts)
}

func TestEnqueueDequeue(t *testing.T) {
	rdb := setupRedis(t)
	registry := tenant.NewRegistry(rdb)
	q := queue.New(rdb, registry)
	ctx := context.Background()

	payload, err := json.Marshal(map[string]string{"to": "test@example.com"})
	require.NoError(t, err)
	j := job.New("send_email", payload, 3)

	require.NoError(t, q.Enqueue(ctx, j))

	got, err := q.Dequeue(ctx, 2*time.Second)
	require.NoError(t, err)

	assert.Equal(t, j.ID, got.ID)
	assert.Equal(t, j.Type, got.Type)
	assert.Equal(t, job.StatusPending, got.Status)
}

func TestDequeue_TimesOutWhenEmpty(t *testing.T) {
	rdb := setupRedis(t)
	registry := tenant.NewRegistry(rdb)
	q := queue.New(rdb, registry)
	ctx := context.Background()

	_, err := q.Dequeue(ctx, 1*time.Second)
	assert.ErrorIs(t, err, redis.Nil)
}

func TestDeadLetter_MoveListRequeue(t *testing.T) {
	rdb := setupRedis(t)
	registry := tenant.NewRegistry(rdb)
	q := queue.New(rdb, registry)
	ctx := context.Background()

	payload, err := json.Marshal(map[string]string{"to": "test@example.com"})
	require.NoError(t, err)
	j := job.New("send_email", payload, 3)
	j.Attempts = 3
	j.LastError = "simulated failure"

	require.NoError(t, q.MoveToDeadLetter(ctx, j))

	jobs, err := q.ListDeadLetter(ctx, 10)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	assert.Equal(t, j.ID, jobs[0].ID)

	require.NoError(t, q.RequeueDeadLetter(ctx, j.ID))

	// after requeue, dead letter should be empty and pending should have it
	jobs, err = q.ListDeadLetter(ctx, 10)
	require.NoError(t, err)
	assert.Empty(t, jobs)

	got, err := q.Dequeue(ctx, 2*time.Second)
	require.NoError(t, err)
	assert.Equal(t, j.ID, got.ID)
	assert.Equal(t, 0, got.Attempts) // confirms attempts was reset on requeue
}

func TestDelayedJobs_PromoteDueJobs(t *testing.T) {
	rdb := setupRedis(t)
	registry := tenant.NewRegistry(rdb)
	q := queue.New(rdb, registry)
	ctx := context.Background()

	payload, err := json.Marshal(map[string]string{"to": "test@example.com"})
	require.NoError(t, err)

	dueJob := job.New("send_email", payload, 3)
	futureJob := job.New("send_email", payload, 3)

	// one job due in the past, one due far in the future
	require.NoError(t, q.EnqueueDelayed(ctx, dueJob, time.Now().Add(-1*time.Second)))
	require.NoError(t, q.EnqueueDelayed(ctx, futureJob, time.Now().Add(1*time.Hour)))

	promoted, err := q.PromoteDueJobs(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, promoted)

	got, err := q.Dequeue(ctx, 2*time.Second)
	require.NoError(t, err)
	assert.Equal(t, dueJob.ID, got.ID)

	// future job should still not be in pending
	_, err = q.Dequeue(ctx, 1*time.Second)
	assert.ErrorIs(t, err, redis.Nil)
}

func TestDequeue_PrioritizesHighOverDefault(t *testing.T) {
	rdb := setupRedis(t)
	registry := tenant.NewRegistry(rdb)
	q := queue.New(rdb, registry)
	ctx := context.Background()

	payload, err := json.Marshal(map[string]string{"to": "test@example.com"})
	require.NoError(t, err)

	lowJob := job.NewWithPriority("send_email", payload, 3, job.PriorityLow)
	highJob := job.NewWithPriority("send_email", payload, 3, job.PriorityHigh)

	// enqueue low first, then high — high should still come out first
	require.NoError(t, q.Enqueue(ctx, lowJob))
	require.NoError(t, q.Enqueue(ctx, highJob))

	got, err := q.Dequeue(ctx, 2*time.Second)
	require.NoError(t, err)
	assert.Equal(t, highJob.ID, got.ID)
}

func TestDependencies_ResolveOnCompletion(t *testing.T) {
	rdb := setupRedis(t)
	registry := tenant.NewRegistry(rdb)
	q := queue.New(rdb, registry)
	ctx := context.Background()

	payload, err := json.Marshal(map[string]string{"to": "test@example.com"})
	require.NoError(t, err)

	upstream := job.New("resize_image", payload, 3)
	downstream := job.NewWithDependencies("send_email", payload, 3, []string{upstream.ID})

	require.NoError(t, q.EnqueueWithDependencies(ctx, downstream))

	// downstream should NOT be runnable yet - nothing in pending
	_, err = q.Dequeue(ctx, 1*time.Second)
	assert.ErrorIs(t, err, redis.Nil)

	require.NoError(t, q.ResolveDependents(ctx, upstream.ID))

	got, err := q.Dequeue(ctx, 2*time.Second)
	require.NoError(t, err)
	assert.Equal(t, downstream.ID, got.ID)
}

func TestDependencies_CascadeFailOnUpstreamDeadLetter(t *testing.T) {
	rdb := setupRedis(t)
	registry := tenant.NewRegistry(rdb)
	q := queue.New(rdb, registry)
	ctx := context.Background()

	payload, err := json.Marshal(map[string]string{"to": "test@example.com"})
	require.NoError(t, err)

	upstream := job.New("resize_image", payload, 3)
	downstream := job.NewWithDependencies("send_email", payload, 3, []string{upstream.ID})

	require.NoError(t, q.EnqueueWithDependencies(ctx, downstream))
	require.NoError(t, q.CascadeFailDependents(ctx, upstream.ID))

	dead, err := q.ListDeadLetter(ctx, 10)
	require.NoError(t, err)
	require.Len(t, dead, 1)
	assert.Equal(t, downstream.ID, dead[0].ID)
	assert.Contains(t, dead[0].LastError, upstream.ID)
}

func TestEnqueueIdempotent_SkipsDuplicateKey(t *testing.T) {
	rdb := setupRedis(t)
	registry := tenant.NewRegistry(rdb)
	q := queue.New(rdb, registry)
	ctx := context.Background()

	payload, err := json.Marshal(map[string]string{"to": "test@example.com"})
	require.NoError(t, err)

	first := job.NewWithIdempotencyKey("send_email", payload, 3, "user-42-welcome")
	second := job.NewWithIdempotencyKey("send_email", payload, 3, "user-42-welcome")

	enqueued1, err := q.EnqueueIdempotent(ctx, first, time.Hour)
	require.NoError(t, err)
	assert.True(t, enqueued1)

	enqueued2, err := q.EnqueueIdempotent(ctx, second, time.Hour)
	require.NoError(t, err)
	assert.False(t, enqueued2) // duplicate, should be skipped

	// only one job should actually be in the pending queue
	got, err := q.Dequeue(ctx, 1*time.Second)
	require.NoError(t, err)
	assert.Equal(t, first.ID, got.ID)

	_, err = q.Dequeue(ctx, 1*time.Second)
	assert.ErrorIs(t, err, redis.Nil) // second one never made it in
}

func TestEnqueueIdempotent_DifferentTypesSameKeyBothEnqueue(t *testing.T) {
	rdb := setupRedis(t)
	registry := tenant.NewRegistry(rdb)
	q := queue.New(rdb, registry)
	ctx := context.Background()

	payload, err := json.Marshal(map[string]string{"to": "test@example.com"})
	require.NoError(t, err)

	emailJob := job.NewWithIdempotencyKey("send_email", payload, 3, "user-42")
	resizeJob := job.NewWithIdempotencyKey("resize_image", payload, 3, "user-42")

	enqueued1, err := q.EnqueueIdempotent(ctx, emailJob, time.Hour)
	require.NoError(t, err)
	assert.True(t, enqueued1)

	enqueued2, err := q.EnqueueIdempotent(ctx, resizeJob, time.Hour)
	require.NoError(t, err)
	assert.True(t, enqueued2) // different type, same key — not a duplicate
}

func FuzzJobMarshalUnmarshalRoundTrip(f *testing.F) {
	f.Add("send_email", `{"to":"test@example.com"}`, 3)
	f.Add("", `{}`, 0)
	f.Add("resize_image", `null`, 1)
	f.Add("a very long job type name that is unusually verbose", `{"key":"value with unicode: 日本語 emoji: 🎉"}`, 100)

	f.Fuzz(func(t *testing.T, jobType string, payload string, maxAttempts int) {
		if !utf8.ValidString(jobType) {
			// JSON text must be valid UTF-8 by spec — encoding/json lossily
			// replaces invalid byte sequences with U+FFFD on marshal, which
			// is expected behavior of the JSON encoding itself, not a bug
			// in Kairos's round-trip logic. job.Type is always populated
			// from Go string literals in real usage (handler registration),
			// never from raw untrusted bytes, so this case isn't realistic.
			t.Skip()
		}

		original := job.New(jobType, json.RawMessage(payload), maxAttempts)

		data, err := json.Marshal(original)
		if err != nil {
			t.Skip()
		}

		var roundTripped job.Job
		if err := json.Unmarshal(data, &roundTripped); err != nil {
			t.Fatalf("failed to unmarshal a job that was just marshaled: %v", err)
		}

		if roundTripped.ID != original.ID {
			t.Errorf("ID mismatch after round-trip: got %q, want %q", roundTripped.ID, original.ID)
		}
		if roundTripped.Type != original.Type {
			t.Errorf("Type mismatch after round-trip: got %q, want %q", roundTripped.Type, original.Type)
		}
	})
}

func TestTenantIsolation_JobsDoNotCrossTenants(t *testing.T) {
	rdb := setupRedis(t)
	registry := tenant.NewRegistry(rdb)
	q := queue.New(rdb, registry)

	ctxA := tenant.WithContext(context.Background(), "tenant-a")
	ctxB := tenant.WithContext(context.Background(), "tenant-b")

	payload, err := json.Marshal(map[string]string{"to": "test@example.com"})
	require.NoError(t, err)
	j := job.New("send_email", payload, 3)

	require.NoError(t, q.Enqueue(ctxA, j))

	// tenant-b's queue must be empty even though tenant-a just enqueued.
	_, err = q.Dequeue(ctxB, 1*time.Second)
	assert.ErrorIs(t, err, redis.Nil)

	// tenant-a can still dequeue its own job.
	got, err := q.Dequeue(ctxA, 2*time.Second)
	require.NoError(t, err)
	assert.Equal(t, j.ID, got.ID)
}

func TestEnqueueBatch_AllJobsLand(t *testing.T) {
	rdb := setupRedis(t)
	registry := tenant.NewRegistry(rdb)
	q := queue.New(rdb, registry)
	ctx := context.Background()

	payload, err := json.Marshal(map[string]string{"to": "test@example.com"})
	require.NoError(t, err)

	jobs := make([]*job.Job, 50)
	for i := range jobs {
		jobs[i] = job.New("send_email", payload, 3)
	}

	require.NoError(t, q.EnqueueBatch(ctx, jobs))

	depth, err := q.Depth(ctx, job.PriorityDefault)
	require.NoError(t, err)
	assert.Equal(t, int64(50), depth)
}

func TestEnqueueBatch_EmptySliceIsNoop(t *testing.T) {
	rdb := setupRedis(t)
	registry := tenant.NewRegistry(rdb)
	q := queue.New(rdb, registry)
	assert.NoError(t, q.EnqueueBatch(context.Background(), nil))
}
