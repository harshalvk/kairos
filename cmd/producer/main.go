// Command producer enqueues a test job onto the queue.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"

	"github.com/harshalvk/kairos/internal/config"
	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/logging"
	"github.com/harshalvk/kairos/internal/queue"
	"github.com/harshalvk/kairos/internal/store"
	"github.com/harshalvk/kairos/internal/tenant"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func main() {

	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}

	ctx := tenant.WithContext(context.Background(), cfg.TenantID)

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	registry := tenant.NewRegistry(rdb)
	q := queue.New(rdb, registry)

	logger := logging.New("producer")
	ctx = logging.WithContext(ctx, logger)

	db, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Error("failed to connect to postgres", slog.Any("error", err))
		os.Exit(1)
	}
	defer db.Close()
	store := store.NewStore(db)

	payload, err := json.Marshal(map[string]string{"to": "devwork2004@gmail.com"})
	if err != nil {
		logger.Error("failed to marshal payload", slog.Any("error", err))
	}
	job := job.NewWithPriority("send_email", payload, 3, job.PriorityHigh)

	if err := q.Enqueue(ctx, job); err != nil {
		logger.Error("failed to enqueue job", slog.Any("error", err))
		os.Exit(1)
	}
	if err := store.RecordCreated(ctx, job); err != nil {
		logger.Error("failed to store record", slog.Any("error", err))
		os.Exit(1)
	}
	logger.Info("enqueued", slog.Any("job_id", job.ID))
}
