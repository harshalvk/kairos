// Command producer enqueues a test job onto the queue.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"

	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/logging"
	"github.com/harshalvk/kairos/internal/queue"
	"github.com/harshalvk/kairos/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func main() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	q := queue.New(rdb)
	ctx := context.Background()
	logger := logging.New("producer")
	ctx = logging.WithContext(ctx, logger)

	pgDSN := os.Getenv("POSTGRES_DSN")
	if pgDSN == "" {
		// #nosec G101
		pgDSN = "postgres://kairos:kairos@localhost:5432/kairos"
	}
	db, err := pgxpool.New(ctx, pgDSN)
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
