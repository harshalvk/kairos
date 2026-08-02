// Package worker implements a concurrent worker pool that dequeues and
// processes jobs, handling retries, dead-lettering, and metrics.
package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/harshalvk/kairos/internal/circuitbreaker"
	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/logging"
	"github.com/harshalvk/kairos/internal/metrics"
	"github.com/harshalvk/kairos/internal/queue"
	"github.com/harshalvk/kairos/internal/ratelimit"
	"github.com/harshalvk/kairos/internal/store"
	"github.com/harshalvk/kairos/internal/tenant"
	"github.com/harshalvk/kairos/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// Handler processes a single job. Returning an error means the job failed.
type Handler func(ctx context.Context, j *job.Job) error

// Pool pulls jobs from a Queue and dispatches them to registered
// Handlers, with a fixed number of concurrent workers.
type Pool struct {
	queue       *queue.Queue
	store       *store.Store
	handlers    map[string]Handler
	concurrency int
	nodeID      string
	limiter     *ratelimit.Limiter
	breaker     *circuitbreaker.Breaker
	tenantID    string
}

// NewPool creates a worker pool with the given concurrency, node
// identifier, and rate limiter (pass ratelimit.New() with no configured limits
// if rate limiting is not needed
func NewPool(queue *queue.Queue, store *store.Store, concurrency int, nodeID string, limiter *ratelimit.Limiter, breaker *circuitbreaker.Breaker, tenantID string) *Pool {
	return &Pool{
		queue:       queue,
		store:       store,
		handlers:    make(map[string]Handler),
		concurrency: concurrency,
		nodeID:      nodeID,
		limiter:     limiter,
		breaker:     breaker,
		tenantID:    tenantID,
	}
}

// RegisterHandler tells the pool which function handles a given job Type.
func (wp *Pool) RegisterHandler(jobType string, h Handler) {
	wp.handlers[jobType] = h
}

// Start launches concurrency goroutines, each pulling jobs in a loop. When
// ctx is cancelled, workers finish their current job (if any) and exit —
// they do not pick up new jobs. Start blocks until every worker has exited
// or shutdownTimeout elapses, whichever comes first.
func (wp *Pool) Start(ctx context.Context, shutdownTimeout time.Duration) {
	logger := logging.FromContext(ctx)
	var wg sync.WaitGroup
	for i := 0; i < wp.concurrency; i++ {
		wg.Add(1)
		workerID := i
		go wp.runWorker(ctx, workerID, &wg)
	}

	// wait for either all workers to finish cleanly, or the timeout to expire
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// wait for the shutdown signal first - workers run indefinitely
	// until thne
	<-ctx.Done()
	logger.Info("shutdown signal received, waiting for in-flight jobs to finish")

	select {
	case <-done:
		logger.Info("all workers exited cleanly")
	case <-time.After(shutdownTimeout):
		logger.Warn("shutdown timeout exceeded, some workers may still be mid-job", slog.Duration("timeout", shutdownTimeout))
	}
}

func (wp *Pool) runWorker(ctx context.Context, id int, wg *sync.WaitGroup) {
	defer wg.Done()
	ctx = tenant.WithContext(ctx, wp.tenantID)
	logger := logging.FromContext(ctx).With(slog.Int("worker_id", id))

	for {
		select {
		case <-ctx.Done():
			logger.Info("shutdown signal received, no longer picking up new jobs")
			return
		default:
		}

		job, err := wp.queue.Dequeue(ctx, 5*time.Second)
		if err != nil {
			continue
		}

		wp.process(ctx, id, job)
	}
}

func (wp *Pool) process(ctx context.Context, workerID int, j *job.Job) {
	ctx, span := tracing.StartSpan(ctx, "job.process")
	defer span.End()
	span.SetAttributes(
		attribute.String("job.id", j.ID),
		attribute.String("job.type", j.Type),
		attribute.Int("job.attempt", j.Attempts+1),
	)

	logger := logging.FromContext(ctx).With(
		slog.Int("workder_id", workerID),
		slog.String("job_id", j.ID),
		slog.String("job_type", j.Type),
		slog.Int("attempt", j.Attempts+1),
		slog.Int("max_attempts", j.MaxAttempts),
	)

	handler, ok := wp.handlers[j.Type]
	if !ok {
		span.SetStatus(codes.Error, "no handler registered")
		logger.Warn("no handler for job type, skipping")
		return
	}

	if !wp.breaker.Allow(j.Type) {
		// circuit is open - this dependency is known to be failing
		// schedule a retry (same backoff mechanism as a normal failure)
		// rather than attempting a call we already expect to fail
		span.AddEvent("circuit_open_deferred")
		logger.Info("circuit open for job type, deferring job")
		wp.scheduleRetry(ctx, j)
		return
	}

	if err := wp.limiter.Wait(ctx, j.Type); err != nil {
		// ctx was cancled while watiting for a rate limit token - likely
		// shutdown in progress. re-queue the job rather than dropping it
		span.RecordError(err)
		logger.Warn("rate limit wait canclled, re-enqueuing", slog.Any("error", err))
		if reErr := wp.queue.Enqueue(ctx, j); reErr != nil {
			logger.Error("failed to re-enqueue after canclled rate-limit wait", slog.Any("error", reErr))
		}
		return
	}

	logger.Info("processing job")

	handlerCtx, handlerSpan := tracing.StartSpan(ctx, "job.handler")
	start := time.Now()
	handlerErr := handler(handlerCtx, j)
	duration := time.Since(start)
	handlerSpan.End()

	metrics.JobDuration.WithLabelValues(j.Type).Observe(duration.Seconds())

	if handlerErr == nil {
		wp.breaker.RecordSuccess(j.Type)
		metrics.CircuitState.WithLabelValues(j.Type).Set(float64(wp.breaker.StateOf(j.Type)))
		j.Status = job.StatusCompleted
		metrics.JobsProcessed.WithLabelValues(j.Type, "completed").Inc()
		if err := wp.store.RecordStatus(ctx, j); err != nil {
			logger.Error("failed to recrod completed status", slog.Any("error", err))
		}
		if err := wp.queue.ResolveDependents(ctx, j.ID); err != nil {
			logger.Error("failed to resolve dependents", slog.Any("error", err))
		}

		span.SetStatus(codes.Ok, "completed")
		logger.Info("job completed", slog.Duration("duration", duration))
		return
	}

	span.RecordError(handlerErr)
	span.SetStatus(codes.Error, handlerErr.Error())

	wp.breaker.RecordFailure(j.Type)
	metrics.CircuitState.WithLabelValues(j.Type).Set(float64(wp.breaker.StateOf(j.Type)))
	j.Attempts++
	j.LastError = handlerErr.Error()
	j.Status = job.StatusFailed
	metrics.JobsProcessed.WithLabelValues(j.Type, "failed").Inc()
	if recError := wp.store.RecordCreated(ctx, j); recError != nil {
		logger.Error("failed to record failed status", slog.Duration("duration", duration))
	}
	logger.Warn("job failed", slog.Any("error", handlerErr), slog.Duration("duration", duration))

	if j.Attempts >= j.MaxAttempts {
		wp.moveToDeadLetter(ctx, j)
		return
	}

	wp.scheduleRetry(ctx, j)
}

func (wp *Pool) scheduleRetry(ctx context.Context, j *job.Job) {
	logger := logging.FromContext(ctx).With(slog.String("job_id", j.ID))
	delay := backoffDuration(j.Attempts)
	runAt := time.Now().Add(delay)
	j.Status = job.StatusPending

	logger.Info("scheduling retry", slog.Time("run_at", runAt), slog.Duration("delay", delay))

	if err := wp.queue.EnqueueDelayed(ctx, j, runAt); err != nil {
		logger.Error("failed to schedule retry", slog.Any("error", err))
	}
}

func backoffDuration(attempt int) time.Duration {
	base := time.Second
	const maxBackoff = 30 * time.Second

	d := base * time.Duration(1<<uint(attempt-1))
	if d > maxBackoff {
		d = maxBackoff
	}
	return d
}

func (wp *Pool) moveToDeadLetter(ctx context.Context, j *job.Job) {
	logger := logging.FromContext(ctx).With(slog.String("job_id", j.ID))
	j.Status = job.StatusDeadLetter

	if err := wp.queue.MoveToDeadLetter(ctx, j); err != nil {
		logger.Error("failed to move to dead letter", slog.Any("error", err))
		return
	}
	metrics.JobsProcessed.WithLabelValues(j.Type, "dead_letter").Inc()
	if err := wp.store.RecordStatus(ctx, j); err != nil {
		logger.Error("failed to record dead-letter status", slog.Any("error", err))
	}
	if err := wp.queue.CascadeFailDependents(ctx, j.ID); err != nil {
		logger.Error("failed to cascade-fail dependents", slog.Any("error", err))
	}

	logger.Warn("job moved to dead-letter", slog.Int("attempts", j.Attempts))
}
