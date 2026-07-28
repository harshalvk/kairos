// Command worker runs the worker pool, processing jobs and serving
// Prometheus metrics.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/harshalvk/kairos/internal/api"
	"github.com/harshalvk/kairos/internal/circuitbreaker"
	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/logging"
	"github.com/harshalvk/kairos/internal/metrics"
	"github.com/harshalvk/kairos/internal/queue"
	"github.com/harshalvk/kairos/internal/ratelimit"
	"github.com/harshalvk/kairos/internal/store"
	"github.com/harshalvk/kairos/internal/tracing"
	"github.com/harshalvk/kairos/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func sendEmailHandler(_ context.Context, job *job.Job) error {
	var payload struct {
		To string `json:"to"`
	}
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return err
	}
	fmt.Printf("sending email to %s (job %s)\n", payload.To, job.ID)
	return nil
}

// // simulated version to fail a job
// func sendEmailHandler(_ context.Context, j *job.Job) error {
// 	time.Sleep(5 * time.Second)
// 	fmt.Printf("email send for job %s\n", j.ID)
// 	return nil
// }

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

	otelEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if otelEndpoint == "" {
		otelEndpoint = "localhost:4318"
	}
	shutdownTracing, err := tracing.Setup(ctx, otelEndpoint, "kairos-worker")
	if err != nil {
		logger.Warn("tracing setup failed, continuing without tracing", slog.Any("error", err))
	} else {
		defer func() {
			if shutdownTracingErr := shutdownTracing(context.Background()); shutdownTracingErr != nil {
				logger.Error("failed to shut down tracing", slog.Any("error", err))
			}
		}()
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	queue := queue.New(rdb)

	limiter := ratelimit.New()
	limiter.SetLimit("send_email", 5, 10) // 5/sec sustained, burst of 10

	pgDSN := os.Getenv("POSTGRES_DSN")
	if pgDSN == "" {
		// #nosec G101 -- local developement default only, not a real
		// credential. always set POSTGRES_DSN in any non-local env
		pgDSN = "postgres://kairos:kairos@localhost:5432/kairos"
	}
	db, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	store := store.NewStore(db)

	breaker := circuitbreaker.New(5, 30*time.Second)                  // open after 5 consecutive fails, 30s cooldown
	pool := worker.NewPool(queue, store, 5, nodeID, limiter, breaker) // 5 concurrent workers
	pool.RegisterHandler("send_email", sendEmailHandler)

	apiServer := api.New(queue, store, logger)
	go func() {
		logger.Info("admin api listening", slog.String("addr", ":8080"))

		srv := &http.Server{
			Addr:              ":8080",
			Handler:           apiServer.Routes(),
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      10 * time.Second,
			IdleTimeout:       60 * time.Second,
		}

		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("admin api server error", slog.Any("error", err))
		}
	}()

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				depth, err := queue.TotalDepth(ctx)
				if err != nil {
					continue
				}
				metrics.QueueDepth.Set(float64(depth))
			}
		}
	}()

	logger.Info("worker pool started", slog.Int("concurrency", 5))
	pool.Start(ctx, 30*time.Second)
	logger.Info("worker pool stopped")
}
