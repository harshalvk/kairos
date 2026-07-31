// Package streamqueue implements an alternavtive job queue backed by
// redis streams instead of plain lists, providing consumer-group
// delivery tracking and crash recovery via XCLAIN - see ADR 0020 for
// how this compares to interanl/queue's list-based implementation
package streamqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/harshalvk/kairos/internal/job"
	"github.com/redis/go-redis/v9"
)

const (
	streamKey = "kairos:stream:pending"
	groupName = "kairos-workers"
	fieldData = "data"
)

// StreamQueue is a job queue backed by a redis stream with a single
// consumer group, so multiple workers can share delivery without
// duplicate processing, and crashed-mid-job messages can be recovered
type StreamQueue struct {
	rdb        *redis.Client
	consumerID string
}

// New creates a StreamQueue and ensures the consumer group exists
// consumerID must be unique per worker process within the group
func New(ctx context.Context, rdb *redis.Client, consumerID string) (*StreamQueue, error) {
	err := rdb.XGroupCreateMkStream(ctx, streamKey, groupName, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return nil, fmt.Errorf("create consumer group: %w", err)
	}

	return &StreamQueue{rdb: rdb, consumerID: consumerID}, nil
}

// Enqueue appends a job to the stream
func (sq *StreamQueue) Enqueue(ctx context.Context, j *job.Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}
	return sq.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]any{fieldData: data},
	}).Err()
}

// Dequeue reads one undelivered message for this consumer, blocking up
// to timeout. the returened messageID must be passed to ack once
// processing succeeds - the message stays in the group's pending
// entires list until then.
func (sq *StreamQueue) Dequeue(ctx context.Context, timeout time.Duration) (*job.Job, string, error) {
	streams, err := sq.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: sq.consumerID,
		Streams:  []string{streamKey, ">"}, // ">" means only new, undelivered messages
		Count:    1,
		Block:    timeout,
	}).Result()
	if err != nil {
		return nil, "", err
	}
	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		return nil, "", redis.Nil
	}

	msg := streams[0].Messages[0]
	raw, ok := msg.Values[fieldData].(string)
	if !ok {
		return nil, "", fmt.Errorf("message %s missing/invalid data field", msg.ID)
	}

	var j job.Job
	if err := json.Unmarshal([]byte(raw), &j); err != nil {
		return nil, "", fmt.Errorf("unmarshal job: %w", err)
	}
	return &j, msg.ID, nil
}

// Ack acknowledges that messageID was successfully processed, removing
// it from the group's pending entries list
func (sq *StreamQueue) Ack(ctx context.Context, messageID string) error {
	return sq.rdb.XAck(ctx, streamKey, groupName, messageID).Err()
}

// ClaimStale reclaims message that were delivered to some consume but
// not acknowledge within minIdleTime - i.e. that consume likely
// crashed mid-processing. clamied messages become owned by this
// consumer and can be reprocessed
func (sq *StreamQueue) ClaimStale(ctx context.Context, minIdleTime time.Duration, count int64) ([]*job.Job, []string, error) {
	messages, _, err := sq.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   streamKey,
		Group:    groupName,
		Consumer: sq.consumerID,
		MinIdle:  minIdleTime,
		Start:    "0",
		Count:    count,
	}).Result()
	if err != nil {
		return nil, nil, fmt.Errorf("autoclaim: %w", err)
	}

	jobs := make([]*job.Job, 0, len(messages))
	ids := make([]string, 0, len(messages))
	for _, msg := range messages {
		raw, ok := msg.Values[fieldData].(string)
		if !ok {
			continue
		}
		var j job.Job
		if err := json.Unmarshal([]byte(raw), &j); err != nil {
			continue
		}
		jobs = append(jobs, &j)
		ids = append(ids, msg.ID)
	}
	return jobs, ids, nil
}

// PendingCount returns how many messages are delivered but unacknowledge
// across the whole consumer group - the size of the "in flight, might be stuck" set
func (sq *StreamQueue) PendingCount(ctx context.Context) (int64, error) {
	summary, err := sq.rdb.XPending(ctx, streamKey, groupName).Result()
	if err != nil {
		return 0, fmt.Errorf("xpending: %w", err)
	}
	return summary.Count, nil
}
