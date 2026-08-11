// Package queue implements a Redis-backed job queue: pending, dead-letter,
// and delayed job storage.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/shard"
	"github.com/harshalvk/kairos/internal/tenant"
	"github.com/redis/go-redis/v9"
)

const (
	waitingKey            = "kairos:waiting"
	waitingCountKeyPrefix = "kairos:waiting:count:"
	dependentsKeyPrefix   = "kairos:dependents:"
)

const idempotencyKeyPrefix = "kairos:idempotency:"

var pendingKeys = map[job.Priority]string{
	job.PriorityHigh:    "kairos:pending:high",
	job.PriorityDefault: "kairos:pending:default",
	job.PriorityLow:     "kairos:pending:low",
}

func waitingCountKey(jobID string) string { return waitingCountKeyPrefix + jobID }
func dependentsKey(jobID string) string   { return dependentsKeyPrefix + jobID }

// Queue wraps a Redis client to provide job enqueue/dequeue operations.
type Queue struct {
	shards   []*redis.Client
	router   *shard.Router
	registry *tenant.Registry
}

// controlShard returns the Redis client used for operations that don't
// belong to a single job type: dead-letter administration and the
// dependency graph (waiting hash, dependents index). Both are
// inherently cross-type — a dead-lettered job or a dependency edge
// between two jobs of different types can't be usefully sharded by a
// single job's type — so they live permanently on shards[0] rather
// than being distributed. This trades away sharding's throughput
// benefit for these specific, comparatively low-volume operations, in
// exchange for a single, always-consistent place to look.
func (q *Queue) controlShard() *redis.Client {
	return q.shards[0]
}

// New creates a Queue backed by the given Redis client.
func New(rdb *redis.Client, registry *tenant.Registry) *Queue {
	return NewSharded([]*redis.Client{rdb}, registry)
}

// NewSharded creates a Queue distributing job types across multiple
// Redis clients via consistent hashing on job type
func NewSharded(rdbs []*redis.Client, registry *tenant.Registry) *Queue {
	return &Queue{
		shards:   rdbs,
		router:   shard.NewRouter(len(rdbs)),
		registry: registry,
	}
}

func (q *Queue) shardFor(jobType string) *redis.Client {
	return q.shards[q.router.ShardFor(jobType)]
}

// Enqueue pushes a job onto the pending queue.
func (q *Queue) Enqueue(ctx context.Context, j *job.Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}
	if err := q.registry.Register(ctx, tenant.FromContext(ctx)); err != nil {
		// non-fatal: registry is a convenience for cross-tenant sweeps
		// like the scheduler, not required for the enqueue itself to work
		return fmt.Errorf("register tenant (enqueue still needs retry): %w", err)
	}

	return q.shardFor(j.Type).LPush(ctx, pendingKey(ctx, j.Priority), data).Err()
}

// Dequeue blocks until a job of one of the given job types is
// available, checking the shard(s) those types route to. A worker
// only needs to know which shards are relevant to the handlers it has
// actually registered.
func (q *Queue) Dequeue(ctx context.Context, jobTypes []string, timeout time.Duration) (*job.Job, error) {
	shardSet := make(map[int]bool)
	for _, t := range jobTypes {
		shardSet[q.router.ShardFor(t)] = true
	}

	// BRPOP across all priority keys on all relevant shards would need
	// multiple blocking calls (Redis BRPOP can't span separate client
	// connections) — so poll each relevant shard's shortest-timeout
	// BRPOP in sequence within the overall timeout budget. Simpler than
	// it sounds: with 1-2 shards per worker (typical), this is a
	// negligible latency cost.
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for shardIdx := range shardSet {
			keys := []string{
				pendingKey(ctx, job.PriorityHigh),
				pendingKey(ctx, job.PriorityDefault),
				pendingKey(ctx, job.PriorityLow),
			}
			result, err := q.shards[shardIdx].BRPop(ctx, 200*time.Millisecond, keys...).Result()
			if err == nil {
				var j job.Job
				if err := json.Unmarshal([]byte(result[1]), &j); err != nil {
					return nil, fmt.Errorf("unmarshal job: %w", err)
				}
				return &j, nil
			}
		}
	}
	return nil, redis.Nil
}

// MoveToDeadLetter stores a permanently-failed job in the dead-letter list.
func (q *Queue) MoveToDeadLetter(ctx context.Context, j *job.Job) error {
	data, err := json.Marshal(j)

	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	return q.controlShard().LPush(ctx, deadLetterKey(ctx), data).Err()
}

// ListDeadLetter returns up to limit dead-lettered jobs without removing
// them. Pass limit = -1 to return all jobs.
func (q *Queue) ListDeadLetter(ctx context.Context, limit int64) ([]*job.Job, error) {
	stop := limit - 1
	if limit < 0 {
		stop = -1
	}

	raw, err := q.controlShard().LRange(ctx, deadLetterKey(ctx), 0, stop).Result()

	if err != nil {
		return nil, fmt.Errorf("lrange dead letter: %w", err)
	}

	jobs := make([]*job.Job, 0, len(raw))

	for _, item := range raw {
		var j job.Job

		if err := json.Unmarshal([]byte(item), &j); err != nil {
			return nil, fmt.Errorf("unmarshal dead letter job: %w", err)
		}

		jobs = append(jobs, &j)
	}

	return jobs, nil
}

// RequeueDeadLetter pulls one job off the dead-letter list and re-enqueues
// it, resetting its attempt count so it gets a fresh set of retries.
func (q *Queue) RequeueDeadLetter(ctx context.Context, jobID string) error {
	jobs, err := q.ListDeadLetter(ctx, -1) // -1 -> all

	if err != nil {
		return err
	}

	for _, j := range jobs {
		if j.ID != jobID {
			continue
		}

		// remove the specific job from the dead-letter list
		data, err := json.Marshal(j)
		if err != nil {
			return fmt.Errorf("marshal job for dead-letter removal: %w", err)
		}

		if err := q.shardFor(j.Type).LRem(ctx, deadLetterKey(ctx), 1, data).Err(); err != nil {
			return fmt.Errorf("remove from dead letter: %w", err)
		}

		j.Attempts = 0
		j.Status = job.StatusPending
		j.LastError = ""

		return q.Enqueue(ctx, j)
	}

	return fmt.Errorf("job %s not found in dead letter queue", jobID)
}

// PurgeDeadLetter deletes all dead-lettered jobs permanently.
func (q *Queue) PurgeDeadLetter(ctx context.Context) error {
	return q.controlShard().Del(ctx, deadLetterKey(ctx)).Err()
}

// EnqueueDelayed schedules a job to become available at runAt, stored in a
// Redis sorted set keyed by Unix timestamp so it survives process restarts.
func (q *Queue) EnqueueDelayed(ctx context.Context, j *job.Job, runAt time.Time) error {
	j.RunAt = runAt
	data, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	return q.shardFor(j.Type).ZAdd(ctx, delayedKey(ctx), redis.Z{
		Score:  float64(runAt.Unix()),
		Member: data,
	}).Err()
}

// PromoteDueJobs finds jobs in the delayed set whose runAt has passed,
// moves them into the pending queue, and removes them from the delayed
// set. Returns how many jobs were promoted.
func (q *Queue) PromoteDueJobs(ctx context.Context) (int, error) {
	now := float64(time.Now().Unix())
	due, err := q.controlShard().ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     delayedKey(ctx),
		ByScore: true,
		Start:   "-inf",
		Stop:    fmt.Sprintf("%f", now),
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("zrangebyscore: %w", err)
	}
	for _, data := range due {
		var j job.Job
		if err := json.Unmarshal([]byte(data), &j); err != nil {
			return 0, fmt.Errorf("unmarshal promoted job: %w", err)
		}
		if err := q.shardFor(j.Type).LPush(ctx, pendingKey(ctx, j.Priority), data).Err(); err != nil {
			return 0, fmt.Errorf("push promoted job: %w", err)
		}
		if err := q.shardFor(j.Type).ZRem(ctx, delayedKey(ctx), data).Err(); err != nil {
			return 0, fmt.Errorf("remove promoted job: %w", err)
		}
	}
	return len(due), nil
}

// Depth returns the current number of pending jobs.
func (q *Queue) Depth(ctx context.Context, p job.Priority) (int64, error) {
	return q.controlShard().LLen(ctx, pendingKey(ctx, p)).Result()
}

// TotalDepth retuns the sum of pending jobs across all priority levels
func (q *Queue) TotalDepth(ctx context.Context) (int64, error) {
	var total int64
	for _, key := range pendingKeys {
		n, err := q.controlShard().LLen(ctx, key).Result()
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

// EnqueueWithDependencies enqueues j immediately if it has no dependencies,
// or parks it in the waiting set until every job in j.DependsOn has completed
func (q *Queue) EnqueueWithDependencies(ctx context.Context, j *job.Job) error {
	if len(j.DependsOn) == 0 {
		return q.Enqueue(ctx, j)
	}

	data, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	pipe := q.shardFor(j.Type).TxPipeline()
	pipe.HSet(ctx, waitingKey, j.ID, data)
	pipe.Set(ctx, waitingCountKey(j.ID), len(j.DependsOn), 0)
	for _, depID := range j.DependsOn {
		pipe.SAdd(ctx, dependentsKey(depID), j.ID)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("enqueue with dependencies: %w", err)
	}
	return nil
}

// ResolveDependents is called when completedJobID finishes successfully.
// it decrements the waiting-dependency count for every job depending on
// it, and enqueues any that now have zero outstanding, dependencies
func (q *Queue) ResolveDependents(ctx context.Context, completedJobID string) error {
	depKey := dependentsKey(completedJobID)
	depnedntIDs, err := q.controlShard().SMembers(ctx, depKey).Result()
	if err != nil {
		return fmt.Errorf("get dependents of %s: %w", completedJobID, err)
	}

	for _, depJobID := range depnedntIDs {
		remaining, err := q.controlShard().Decr(ctx, waitingCountKey(depJobID)).Result()
		if err != nil {
			return fmt.Errorf("decrement waiting count for %s: %w", depJobID, err)
		}
		if remaining > 0 {
			continue
		}

		data, err := q.controlShard().HGet(ctx, waitingKey, depJobID).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue // already promoted (e.g. by a concurrent resolve)
			}
			return fmt.Errorf("get waiting job %s: %w", depJobID, err)
		}

		var readyJob job.Job
		if err := json.Unmarshal([]byte(data), &readyJob); err != nil {
			return fmt.Errorf("unmarshal waiting job %s: %w", depJobID, err)
		}

		if err := q.Enqueue(ctx, &readyJob); err != nil {
			return fmt.Errorf("enqueue ready job %s: %w", depJobID, err)
		}

		cleanup := q.controlShard().TxPipeline()
		cleanup.HDel(ctx, waitingKey, depJobID)
		cleanup.Del(ctx, waitingCountKey(depJobID))
		if _, err := cleanup.Exec(ctx); err != nil {
			return fmt.Errorf("cleanup waiting state for %s: %w", depJobID, err)
		}
	}

	return q.controlShard().Del(ctx, depKey).Err()
}

// CascadeFailDependents moves every job waiting on failedJobID - directly
// or transitively - to the dead-letter queue, since a permanently failed
// dependency means they can never legitimately run
func (q *Queue) CascadeFailDependents(ctx context.Context, failedJobID string) error {
	toVisit := []string{failedJobID}

	for len(toVisit) > 0 {
		id := toVisit[0]
		toVisit = toVisit[1:]

		depKey := dependentsKey(id)
		dependentIDs, err := q.controlShard().SMembers(ctx, depKey).Result()
		if err != nil {
			return fmt.Errorf("get dependents of %s: %w", id, err)
		}

		for _, depJobID := range dependentIDs {
			data, err := q.controlShard().HGet(ctx, waitingKey, depJobID).Result()
			if err != nil {
				if errors.Is(err, redis.Nil) {
					continue
				}
				return fmt.Errorf("get waiting job %s: %w", depJobID, err)
			}

			var waitingJob job.Job
			if err := json.Unmarshal([]byte(data), &waitingJob); err != nil {
				return fmt.Errorf("unmarshal waiting job %s: %w", depJobID, err)
			}
			waitingJob.Status = job.StatusDeadLetter
			waitingJob.LastError = fmt.Sprintf("upstream dependency %s failed permanently", id)

			if err := q.MoveToDeadLetter(ctx, &waitingJob); err != nil {
				return fmt.Errorf("move %s to dead letter: %w", depJobID, err)
			}

			cleanup := q.controlShard().TxPipeline()
			cleanup.HDel(ctx, waitingKey, depJobID)
			cleanup.Del(ctx, waitingCountKey(depJobID))
			if _, err := cleanup.Exec(ctx); err != nil {
				return fmt.Errorf("cleanup waiting state for %s: %w", depJobID, err)
			}

			toVisit = append(toVisit, depJobID) // cascade further down the chain
		}

		if err := q.controlShard().Del(ctx, depKey).Err(); err != nil {
			return fmt.Errorf("cleanup dependencies key for %s: %w", id, err)
		}
	}

	return nil
}

func idempotencyRedisKey(jobType, key string) string {
	// Scoped by job type so the same key can be reused across different
	// job types without colliding — "user-123" as an idempotency key for
	// send_email shouldn't block "user-123" for resize_image.
	return idempotencyKeyPrefix + jobType + ":" + key
}

// EnqueueIdempotent enqueues j only if no job with the same Type and
// IdemotencyKey has been enqueued within ttl. returns (true, nil) if the
// job was actually enqueued, (false, nil) if it was a duplicate and
// silently skipped. if j.IdempotencyKey is empty, it always enqueus
// (idempotency is opt-in per job)
func (q *Queue) EnqueueIdempotent(ctx context.Context, j *job.Job, ttl time.Duration) (bool, error) {
	if j.IdempotencyKey == "" {
		return true, q.Enqueue(ctx, j)
	}

	redisKey := idempotencyRedisKey(j.Type, j.IdempotencyKey)

	// SET NX: only succeed if the key doesn't already exist. this is the
	// atomic "claim" opertion - two concurrent producers reacing to
	// enqueue the same idempotency key will have exactly one SETNX
	// succeed, so there's no window for both to slip through
	acquired, err := q.shardFor(j.Type).SetNX(ctx, redisKey, j.ID, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("idempotency check: %w", err)
	}
	if !acquired {
		return false, nil // duplicate - already claimed by an earler enqueue
	}

	if err := q.Enqueue(ctx, j); err != nil {
		// Enqueue failed after we clamied the key - release the claim so a
		// legitimate retry isn't permanently blocked by our own failure
		if delErr := q.shardFor(j.Type).Del(ctx, redisKey).Err(); delErr != nil {
			return false, fmt.Errorf("enqueue failed (%v), and failed to release idempotency claim: %w", err, delErr)
		}
		return false, fmt.Errorf("enqueue after idempotency claim: %w", err)
	}

	return true, nil
}

func pendingKey(ctx context.Context, p job.Priority) string {
	t := tenant.FromContext(ctx)
	base := map[job.Priority]string{
		job.PriorityHigh:    "pending:high",
		job.PriorityDefault: "pending:default",
		job.PriorityLow:     "pending:low",
	}[p]
	if base == "" {
		base = "pending:default"
	}
	return fmt.Sprintf("kairos:%s:%s", t, base)
}

func deadLetterKey(ctx context.Context) string {
	return fmt.Sprintf("kairos:%s:dead_letter", tenant.FromContext(ctx))
}

func delayedKey(ctx context.Context) string {
	return fmt.Sprintf("kairos:%s:delayed", tenant.FromContext(ctx))
}

// EnqueueBatch pushes all of jobs onto their priority-appropriate
// pending queus in a single pipelined round-trip, rather than one
// round-trip per job
func (q *Queue) EnqueueBatch(ctx context.Context, jobs []*job.Job) error {
	if len(jobs) == 0 {
		return nil
	}

	pipe := q.controlShard().Pipeline()
	for _, j := range jobs {
		data, err := json.Marshal(j)
		if err != nil {
			return fmt.Errorf("marshal job %s: %w", j.ID, err)
		}
		pipe.LPush(ctx, pendingKey(ctx, j.Priority), data)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("enqueue batch: %w", err)
	}

	if err := q.registry.Register(ctx, tenant.FromContext(ctx)); err != nil {
		return fmt.Errorf("register tenant: %w", err)
	}
	return nil
}
