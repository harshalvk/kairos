// Command cron runs recurring job defintions on their configured cron
// schedules, enqueuing a fresh job instance each time on fires
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/robfig/cron/v3"

	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/logging"
	"github.com/harshalvk/kairos/internal/queue"
	"github.com/harshalvk/kairos/internal/scheduler"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := logging.New("cron")
	ctx = logging.WithContext(ctx, logger)

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	q := queue.New(rdb)

	pgDSN := os.Getenv("POSTGRES_DSN")
	if pgDSN == "" {
		// #nosec G101
		pgDSN = "postgres://kairos:kairos@localhost:5432/kairos"
	}
	db, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		logger.Error("failed to connect to postgrs", slog.Any("error", err))
		os.Exit(1)
	}
	defer db.Close()
	store := scheduler.NewStore(db)

	recurringJobs, err := store.ListEnabled(ctx)
	if err != nil {
		logger.Error("failed to list recurring jobs", slog.Any("error", err))
		os.Exit(1)
	}
	logger.Info("loaded enabled recurring jobs", slog.Int("count", len(recurringJobs)))

	c := cron.New(cron.WithSeconds())

	for _, rj := range recurringJobs {
		rj := rj // capture loop varialbe for the closure below
		_, err := c.AddFunc(rj.CronExpr, func() {
			j := job.New(rj.JobType, rj.Payload, rj.MaxAttempts)
			if err := q.Enqueue(ctx, j); err != nil {
				logger.Error("recurring job failed to enqueue", slog.String("job_name", rj.Name), slog.Any("error", err))
				return
			}
			if err := store.RecordRun(ctx, rj.ID, j.CreatedAt); err != nil {
				logger.Error("recurring job failed to record run", slog.String("job_name", rj.Name), slog.Any("error", err))
			}
			logger.Info("recurring job fired", slog.String("job_name", rj.Name), slog.String("job_id", j.ID))
		})
		if err != nil {
			logger.Warn("invalid cron expression", slog.String("job_name", rj.Name), slog.String("cron_expr", rj.CronExpr), slog.Any("error", err))
			continue
		}
	}

	c.Start()
	logger.Info("cron scheduler started")

	<-ctx.Done()
	logger.Info("shutting down cron scheduler...")
	stopCtx := c.Stop() // stops accepting new triggers, waits for running jobs
	<-stopCtx.Done()
	logger.Info("cron scheduler stopped")
}
