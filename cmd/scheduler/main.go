// Command scheduler polls the delayed queue and promotes due jobs to
// the pending queue.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/harshalvk/kairos/internal/leaderelection"
	"github.com/harshalvk/kairos/internal/logging"
	"github.com/harshalvk/kairos/internal/queue"
	"github.com/harshalvk/kairos/internal/tenant"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			hostname = "unknown"
		}
		nodeID = hostname
	}

	logger := logging.New(nodeID)
	ctx = logging.WithContext(ctx, logger)

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	registry := tenant.NewRegistry(rdb)
	q := queue.New(rdb, registry)

	const (
		lockTTL         = 15 * time.Second
		renewInterval   = 5 * time.Second
		promoteInterval = 1 * time.Second
	)
	elector := leaderelection.New(rdb, nodeID, lockTTL, logger)

	defer func() {
		if err := elector.Release(context.Background()); err != nil {
			logger.Warn("failed to release leadership on shutdown", slog.Any("error", err))
		}
	}()

	acquireTicker := time.NewTicker(renewInterval)
	defer acquireTicker.Stop()
	promoteTicker := time.NewTicker(promoteInterval)
	defer promoteTicker.Stop()

	logger.Info("scheduler started, attempting to acquire leadership")

	for {
		select {
		case <-ctx.Done():
			logger.Info("shutting down scheduler")
			return

		case <-acquireTicker.C:
			if !elector.IsLeader() {
				elector.TryAcquire(ctx)
			} else if err := elector.Renew(ctx); err != nil {
				logger.Warn("failed to renew leadership", slog.Any("error", err))
			}

		case <-promoteTicker.C:
			if !elector.IsLeader() {
				continue
			}

			tenants, err := registry.List(ctx)
			if err != nil {
				logger.Error("failed to list tenants", slog.Any("error", err))
				continue
			}
			for _, t := range tenants {
				tenantCtx := tenant.WithContext(ctx, t)
				n, err := q.PromoteDueJobs(tenantCtx)
				if err != nil {
					logger.Error("promote due jobs failed", slog.String("tenant_id", t), slog.Any("error", err))
					continue
				}
				if n > 0 {
					logger.Info("promoted due jobs", slog.String("tenant_id", t), slog.Int("count", n))
				}
			}
		}
	}
}
