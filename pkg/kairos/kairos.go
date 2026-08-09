// Package kairos is the ergonomic, public entry point to a Kairos
// deployment — wrapping queue, worker, store, rate limiting, and the
// circuit breaker behind a small, fluent API. Internal packages stay
// internal; this is the one surface most users should ever import.
package kairos

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/harshalvk/kairos/internal/circuitbreaker"
	ijob "github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/logging"
	"github.com/harshalvk/kairos/internal/queue"
	"github.com/harshalvk/kairos/internal/ratelimit"
	"github.com/harshalvk/kairos/internal/store"
	"github.com/harshalvk/kairos/internal/tenant"
	"github.com/harshalvk/kairos/internal/webhook"
	"github.com/harshalvk/kairos/internal/worker"
)

// Job is what a handler receives — a thin, ergonomic view over the
// internal job.Job, with a Bind helper so handlers don't hand-roll
// json.Unmarshal every time.
type Job struct {
	ID      string
	Type    string
	Attempt int
	raw     []byte
	inner   *ijob.Job
}

// Bind unmarshals the job's payload into v.
func (j Job) Bind(v any) error {
	return json.Unmarshal(j.raw, v)
}

// HandlerFunc processes one job. Returning an error triggers a retry
// (or dead-lettering, once attempts are exhausted).
type HandlerFunc func(ctx context.Context, j Job) error

// Kairos is the top-level client: register handlers, enqueue jobs, run.
type Kairos struct {
	cfg        config
	rdb        *redis.Client
	db         *pgxpool.Pool
	queue      *queue.Queue
	store      *store.Store
	limiter    *ratelimit.Limiter
	breaker    *circuitbreaker.Breaker
	logger     *slog.Logger
	pool       *worker.Pool
	dispatcher *webhook.Dispatcher
}

// New connects to Redis (and Postgres, if configured) and returns a
// ready-to-use client. Nothing runs yet — call Handle to register work,
// then Run to start processing.
func New(opts ...Option) (*Kairos, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	logger := cfg.logger
	if logger == nil {
		logger = logging.New(cfg.nodeID)
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.redisAddr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("kairos: connect to redis: %w", err)
	}
	registry := tenant.NewRegistry(rdb)
	q := queue.New(rdb, registry)

	var s *store.Store
	if cfg.postgresDSN != "" {
		db, err := pgxpool.New(context.Background(), cfg.postgresDSN)
		if err != nil {
			return nil, fmt.Errorf("kairos: connect to postgres: %w", err)
		}
		s = store.NewStore(db)
	}

	limiter := ratelimit.New()
	breaker := circuitbreaker.New(cfg.circuitThreshold, cfg.circuitCooldown)
	dispatcher := webhook.New(rdb, logger)
	pool := worker.NewPool(q, s, cfg.concurrency, cfg.nodeID, limiter, breaker, cfg.tenantID, dispatcher)

	return &Kairos{
		cfg: cfg, rdb: rdb, queue: q, store: s,
		limiter: limiter, breaker: breaker, logger: logger, pool: pool,
		dispatcher: dispatcher,
	}, nil
}

// Handle registers a HandlerFunc for jobType. Call before Run.
func (k *Kairos) Handle(jobType string, fn HandlerFunc, opts ...JobOption) {
	jo := jobOptions{maxAttempts: 3, priority: ijob.PriorityDefault}
	for _, opt := range opts {
		opt(&jo)
	}
	if jo.rateLimit > 0 {
		k.limiter.SetLimit(jobType, jo.rateLimit, jo.rateBurst)
	}
	k.pool.RegisterHandler(jobType, func(ctx context.Context, j *ijob.Job) error {
		return fn(ctx, Job{ID: j.ID, Type: j.Type, Attempt: j.Attempts + 1, raw: j.Payload, inner: j})
	})
}

// Enqueue submits a job of jobType. payload is JSON-marshaled
// automatically — pass a struct, map, or anything encoding/json accepts.
func (k *Kairos) Enqueue(ctx context.Context, jobType string, payload any, opts ...EnqueueOption) (string, error) {
	eo := enqueueOptions{maxAttempts: 3, priority: ijob.PriorityDefault}
	for _, opt := range opts {
		opt(&eo)
	}

	ctx = tenant.WithContext(ctx, k.cfg.tenantID)

	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("kairos: marshal payload: %w", err)
	}

	var j *ijob.Job
	switch {
	case len(eo.dependsOn) > 0:
		j = ijob.NewWithDependencies(jobType, data, eo.maxAttempts, eo.dependsOn)
		j.Priority = eo.priority
		if err := k.queue.EnqueueWithDependencies(ctx, j); err != nil {
			return "", fmt.Errorf("kairos: enqueue: %w", err)
		}
	case eo.idempotencyKey != "":
		j = ijob.NewWithIdempotencyKey(jobType, data, eo.maxAttempts, eo.idempotencyKey)
		j.Priority = eo.priority
		if _, err := k.queue.EnqueueIdempotent(ctx, j, 24*time.Hour); err != nil {
			return "", fmt.Errorf("kairos: enqueue: %w", err)
		}
	default:
		j = ijob.NewWithPriority(jobType, data, eo.maxAttempts, eo.priority)
		if err := k.queue.Enqueue(ctx, j); err != nil {
			return "", fmt.Errorf("kairos: enqueue: %w", err)
		}
	}

	if k.store != nil {
		if err := k.store.RecordCreated(ctx, j); err != nil {
			k.logger.Warn("failed to record job creation", slog.Any("error", err))
		}
	}
	return j.ID, nil
}

// Run starts the worker pool and blocks until ctx is cancelled (e.g. on
// SIGINT/SIGTERM if you pass a signal.NotifyContext), then drains
// in-flight jobs within shutdownTimeout before returning.
func (k *Kairos) Run(ctx context.Context, shutdownTimeout time.Duration) {
	ctx = tenant.WithContext(ctx, k.cfg.tenantID)
	ctx = logging.WithContext(ctx, k.logger)

	go k.dispatcher.Run(ctx)

	k.pool.Start(ctx, shutdownTimeout)
}

// Close releases underlying connections.
func (k *Kairos) Close() error {
	if k.db != nil {
		k.db.Close()
	}
	return k.rdb.Close()
}

// Result retrieves the json result of jobID, if the handler set one via
// Job.SetResult. requires WithPostgresDSN to have been configured
func (k *Kairos) Result(ctx context.Context, jobID string, out any) error {
	if k.store == nil {
		return fmt.Errorf("kairos: Result requires WithPostgresDSN")
	}
	ctx = tenant.WithContext(ctx, k.cfg.tenantID)
	raw, err := k.store.GetResult(ctx, jobID)
	if err != nil {
		return err
	}
	if raw == nil {
		return fmt.Errorf("kairos: job %s has no result (not completed, or handler none)", jobID)
	}
	return json.Unmarshal(raw, out)
}

// SetResult attaches a result to the job, persisted once it completes
// successfully (requires WithPostgresDSN). Retrieve it later via
// Kairos.Result.
func (j Job) SetResult(v any) error {
	return j.inner.SetResult(v)
}

// EnqueueBatch submits any jobs of jobType in a single redis round-trip
// -- significantly faster than calling enqueus in a loop for bulk work
// (e,g. importing a csv). dependencies and idempotency keys are not
// supported per-item in a batch; use enqueue for those
func (k *Kairos) EnqueueBatch(ctx context.Context, jobType string, payloads []any, opts ...EnqueueOption) ([]string, error) {
	eo := enqueueOptions{maxAttempts: 3, priority: ijob.PriorityDefault}
	for _, opt := range opts {
		opt(&eo)
	}
	ctx = tenant.WithContext(ctx, k.cfg.tenantID)

	jobs := make([]*ijob.Job, len(payloads))
	ids := make([]string, len(payloads))
	for i, p := range payloads {
		data, err := json.Marshal(p)
		if err != nil {
			return nil, fmt.Errorf("kairos: marshal payload %d: %w", i, err)
		}
		j := ijob.NewWithPriority(jobType, data, eo.maxAttempts, eo.priority)
		jobs[i] = j
		ids[i] = j.ID
	}

	if err := k.queue.EnqueueBatch(ctx, jobs); err != nil {
		return nil, fmt.Errorf("kairos: enqueue batch: %w", err)
	}
	return ids, nil
}
