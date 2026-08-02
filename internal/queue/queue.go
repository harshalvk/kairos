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

func keyFor(p job.Priority) string {
	if key, ok := pendingKeys[p]; ok {
		return key
	}
	return pendingKeys[job.PriorityDefault] // unknown priority falls back to default
}

// Queue wraps a Redis client to provide job enqueue/dequeue operations.
type Queue struct {
	rdb *redis.Client
}

// New creates a Queue backed by the given Redis client.
func New(rdb *redis.Client) *Queue {
	return &Queue{rdb}
}

// Enqueue pushes a job onto the pending queue.
func (q *Queue) Enqueue(ctx context.Context, j *job.Job) error {
	data, err := json.Marshal(j)

	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	return q.rdb.LPush(ctx, pendingKey(ctx, j.Priority), data).Err()
}

// Dequeue blocks until a job is available, then returns it.
// A timeout of 0 means block forever.
func (q *Queue) Dequeue(ctx context.Context, timeout time.Duration) (*job.Job, error) {
	keys := []string{
		pendingKey(ctx, job.PriorityHigh),
		pendingKey(ctx, job.PriorityDefault),
		pendingKey(ctx, job.PriorityLow),
	}
	result, err := q.rdb.BRPop(ctx, timeout, keys...).Result()

	if err != nil {
		return nil, err
	}

	var j job.Job
	if err := json.Unmarshal([]byte(result[1]), &j); err != nil {
		return nil, fmt.Errorf("unmarshal job: %w", err)
	}

	return &j, nil
}

// MoveToDeadLetter stores a permanently-failed job in the dead-letter list.
func (q *Queue) MoveToDeadLetter(ctx context.Context, j *job.Job) error {
	data, err := json.Marshal(j)

	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	return q.rdb.LPush(ctx, deadLetterKey(ctx), data).Err()
}

// ListDeadLetter returns up to limit dead-lettered jobs without removing
// them. Pass limit = -1 to return all jobs.
func (q *Queue) ListDeadLetter(ctx context.Context, limit int64) ([]*job.Job, error) {
	stop := limit - 1
	if limit < 0 {
		stop = -1
	}

	raw, err := q.rdb.LRange(ctx, deadLetterKey(ctx), 0, stop).Result()

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

		if err := q.rdb.LRem(ctx, deadLetterKey(ctx), 1, data).Err(); err != nil {
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
	return q.rdb.Del(ctx, deadLetterKey(ctx)).Err()
}

// EnqueueDelayed schedules a job to become available at runAt, stored in a
// Redis sorted set keyed by Unix timestamp so it survives process restarts.
func (q *Queue) EnqueueDelayed(ctx context.Context, j *job.Job, runAt time.Time) error {
	j.RunAt = runAt
	data, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	return q.rdb.ZAdd(ctx, delayedKey(ctx), redis.Z{
		Score:  float64(runAt.Unix()),
		Member: data,
	}).Err()
}

// PromoteDueJobs finds jobs in the delayed set whose runAt has passed,
// moves them into the pending queue, and removes them from the delayed
// set. Returns how many jobs were promoted.
func (q *Queue) PromoteDueJobs(ctx context.Context) (int, error) {
	now := float64(time.Now().Unix())

	// fetches all delayed jobs with the socre <= now (i.e due to run)
	due, err := q.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     delayedKey(ctx),
		Start:   "-inf",
		Stop:    fmt.Sprintf("%f", now),
		ByScore: true,
	}).Result()

	if err != nil {
		return 0, fmt.Errorf("zrangebyscore: %w", err)
	}

	for _, data := range due {
		var j job.Job
		if err := json.Unmarshal([]byte(data), &j); err != nil {
			return 0, fmt.Errorf("unmarshal due job: %w", err)
		}
		if err := q.rdb.LPush(ctx, keyFor(j.Priority), data).Err(); err != nil {
			return 0, fmt.Errorf("push promoted job: %w", err)
		}
		if err := q.rdb.ZRem(ctx, delayedKey(ctx), data).Err(); err != nil {
			return 0, fmt.Errorf("remove promoted job: %w", err)
		}
	}

	return len(due), nil
}

// Depth returns the current number of pending jobs.
func (q *Queue) Depth(ctx context.Context, p job.Priority) (int64, error) {
	return q.rdb.LLen(ctx, keyFor(p)).Result()
}

// TotalDepth retuns the sum of pending jobs across all priority levels
func (q *Queue) TotalDepth(ctx context.Context) (int64, error) {
	var total int64
	for _, key := range pendingKeys {
		n, err := q.rdb.LLen(ctx, key).Result()
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

	pipe := q.rdb.TxPipeline()
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
	depnedntIDs, err := q.rdb.SMembers(ctx, depKey).Result()
	if err != nil {
		return fmt.Errorf("get dependents of %s: %w", completedJobID, err)
	}

	for _, depJobID := range depnedntIDs {
		remaining, err := q.rdb.Decr(ctx, waitingCountKey(depJobID)).Result()
		if err != nil {
			return fmt.Errorf("decrement waiting count for %s: %w", depJobID, err)
		}
		if remaining > 0 {
			continue
		}

		data, err := q.rdb.HGet(ctx, waitingKey, depJobID).Result()
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

		cleanup := q.rdb.TxPipeline()
		cleanup.HDel(ctx, waitingKey, depJobID)
		cleanup.Del(ctx, waitingCountKey(depJobID))
		if _, err := cleanup.Exec(ctx); err != nil {
			return fmt.Errorf("cleanup waiting state for %s: %w", depJobID, err)
		}
	}

	return q.rdb.Del(ctx, depKey).Err()
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
		dependentIDs, err := q.rdb.SMembers(ctx, depKey).Result()
		if err != nil {
			return fmt.Errorf("get dependents of %s: %w", id, err)
		}

		for _, depJobID := range dependentIDs {
			data, err := q.rdb.HGet(ctx, waitingKey, depJobID).Result()
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

			cleanup := q.rdb.TxPipeline()
			cleanup.HDel(ctx, waitingKey, depJobID)
			cleanup.Del(ctx, waitingCountKey(depJobID))
			if _, err := cleanup.Exec(ctx); err != nil {
				return fmt.Errorf("cleanup waiting state for %s: %w", depJobID, err)
			}

			toVisit = append(toVisit, depJobID) // cascade further down the chain
		}

		if err := q.rdb.Del(ctx, depKey).Err(); err != nil {
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
	acquired, err := q.rdb.SetNX(ctx, redisKey, j.ID, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("idempotency check: %w", err)
	}
	if !acquired {
		return false, nil // duplicate - already claimed by an earler enqueue
	}

	if err := q.Enqueue(ctx, j); err != nil {
		// Enqueue failed after we clamied the key - release the claim so a
		// legitimate retry isn't permanently blocked by our own failure
		if delErr := q.rdb.Del(ctx, redisKey).Err(); delErr != nil {
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
