// Command producer enqueues a test job onto the queue.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"

	"github.com/harshalvk/kairos/internal/logging"
	"github.com/harshalvk/kairos/internal/queue"
	"github.com/redis/go-redis/v9"
)

func main() {
	action := flag.String("action", "list", "list | requeue | purge")
	jobID := flag.String("id", "", "job ID (required for requeue)")
	flag.Parse()

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	q := queue.New(rdb)
	ctx := context.Background()
	logger := logging.New("deadletter")
	ctx = logging.WithContext(ctx, logger)

	switch *action {
	case "list":
		jobs, err := q.ListDeadLetter(ctx, 50)
		if err != nil {
			logger.Error("failed list list deadletter jobs", slog.Any("error", err))
			os.Exit(1)
		}
		for _, j := range jobs {
			logger.Info("dead-lettered job",
				slog.String("job_id", j.ID),
				slog.String("job_type", j.Type),
				slog.Int("attempts", j.Attempts),
				slog.String("last_error", j.LastError),
			)
		}
	case "requeue":
		if *jobID == "" {
			logger.Info("--id required for requeue")
			os.Exit(1)
		}
		if err := q.RequeueDeadLetter(ctx, *jobID); err != nil {
			logger.Error("failed to requeue to deadletter", slog.Any("error", err))
			os.Exit(1)
		}

		logger.Info("requeued: ", slog.String("job_id", *jobID))
	case "purge":
		if err := q.PurgeDeadLetter(ctx); err != nil {
			logger.Error("failed to purge dead letter", slog.Any("error", err))
			os.Exit(1)
		}
		logger.Info("dead letter queue purged")
	default:
		logger.Error("unknown action", slog.String("action", *action))
		os.Exit(1)
	}
}
