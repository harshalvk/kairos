// Command scheduler polls the delayed queue and promotes due jobs to
// the pending queue.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/harshalvk/kairos/internal/logging"
	"github.com/harshalvk/kairos/internal/queue"
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
	logger := logging.New("scheduler")
	ctx = logging.WithContext(ctx, logger)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	logger.Info("scheduler started, checking for due jobs every 1s")
	for range ticker.C {
		n, err := q.PromoteDueJobs(ctx)
		if err != nil {
			logger.Error("promote due jobs", slog.Any("error", err))
			continue
		}
		if n > 0 {
			logger.Info("promoted due job(s) to pending queue", slog.Int("promoted_due_job(s)", n))
		}
	}
}
