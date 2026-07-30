// Package leaderelection provides a simple redis-based distributed lock
// so exactly one instance of a relicated process (e.g. the scheduler)
// is active at a time
package leaderelection

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

const lockKey = "kairos:leader:scheduler"

// Elector manages leader election via a redis lock with a TTL, renewed
// periodically while held
type Elector struct {
	rdb      *redis.Client
	nodeID   string
	ttl      time.Duration
	logger   *slog.Logger
	isLeader bool
}

// New creates an electror. nodeID identifies this process in the lock
// value (useful for debugging who curretnly hold leadership). ttl
// should be meaningfully longer than the renewal internal you plan to
// use, so a brief renewal dealy doesn't cause an unnecessary handoff
func New(rdb *redis.Client, nodeID string, ttl time.Duration, logger *slog.Logger) *Elector {
	return &Elector{rdb: rdb, nodeID: nodeID, ttl: ttl, logger: logger}
}

// TryAcquire attempts to become leader. returns true if this call
// acquired or already held leadership
func (e *Elector) TryAcquire(ctx context.Context) bool {
	acquired, err := e.rdb.SetNX(ctx, lockKey, e.nodeID, e.ttl).Result()
	if err != nil {
		e.logger.Error("leader election: acquire attempt failed", slog.Any("error", err))
		e.isLeader = false
		return false

	}
	if acquired {
		e.isLeader = true
		e.logger.Info("acquired leadership")
		return true
	}
	e.isLeader = false
	return false
}

// Renew extends the lock's TTL if this node currently holds it. Must be
// called periodically (well before ttl elapses) by whichever node holds
// leadership, or it will lose leadership when the TTL expires.
func (e *Elector) Renew(ctx context.Context) error {
	if !e.isLeader {
		return errors.New("cannot renew: not currently leader")
	}

	script := redis.NewScript(`
		if redis.call('GET', KEYS[1]) == ARGV[1] then
			return redis.call('EXPIRE', KEYS[1], ARGV[2])
		else
			return 0
		end
	`)

	result, err := script.Run(ctx, e.rdb, []string{lockKey}, e.nodeID, int(e.ttl.Seconds())).Result()
	if err != nil {
		return err
	}
	renewed, ok := result.(int64)
	if !ok {
		return errors.New("unexpected result type %T from script")
	}
	if renewed == 0 {
		e.isLeader = false
		return errors.New("lost leadership: lock held by another node or expired")
	}
	return nil
}

// Release voluntarily gives up leadership, e.g. on graceful shutdown, so
// another node can take over immediately instead of waiting for the TTL
// to expire.
func (e *Elector) Release(ctx context.Context) error {
	if !e.isLeader {
		return nil
	}

	script := redis.NewScript(`
		if redis.call('GET', KEYS[1]) == ARGV[1] then
			return redis.call('DEL', KEYS[1])
		else
			return 0
		end
	`)
	_, err := script.Run(ctx, e.rdb, []string{lockKey}, e.nodeID).Result()
	e.isLeader = false
	return err
}

// IsLeader reports whether this node curretnly believes it hold
// leadership (based on the last TryAcquire/Renew result, not a live
// redis check)
func (e *Elector) IsLeader() bool {
	return e.isLeader
}
