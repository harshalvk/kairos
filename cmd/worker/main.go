// Command worker runs the worker pool, processing jobs and serving
// Prometheus metrics.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/harshalvk/kairos/internal/api"
	"github.com/harshalvk/kairos/internal/circuitbreaker"
	"github.com/harshalvk/kairos/internal/config"
	"github.com/harshalvk/kairos/internal/grpcserver"
	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/logging"
	"github.com/harshalvk/kairos/internal/metrics"
	"github.com/harshalvk/kairos/internal/queue"
	"github.com/harshalvk/kairos/internal/ratelimit"
	"github.com/harshalvk/kairos/internal/store"
	"github.com/harshalvk/kairos/internal/tenant"
	"github.com/harshalvk/kairos/internal/tracing"
	"github.com/harshalvk/kairos/internal/worker"
	"github.com/harshalvk/kairos/pkg/kairospb"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
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
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	nodeID := cfg.NodeID
	logger := logging.New(nodeID)
	ctx = logging.WithContext(ctx, logger)

	shutdownTracing, err := tracing.Setup(ctx, cfg.OTELEndpoint, "kairos-worker")
	if err != nil {
		logger.Warn("tracing setup failed, continuing without tracing", slog.Any("error", err))
	} else {
		defer func() {
			if shutdownTracingErr := shutdownTracing(context.Background()); shutdownTracingErr != nil {
				logger.Error("failed to shut down tracing", slog.Any("error", err))
			}
		}()
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	registry := tenant.NewRegistry(rdb)
	queue := queue.New(rdb, registry)

	limiter := ratelimit.New()
	limiter.SetLimit("send_email", 5, 10) // 5/sec sustained, burst of 10

	db, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	store := store.NewStore(db)

	breaker := circuitbreaker.New(5, 30*time.Second) // open after 5 consecutive fails, 30s cooldown

	tenantID := cfg.TenantID
	if err := tenant.Validate(tenantID); err != nil {
		logger.Error("invalid TENANT_ID", slog.Any("error", err))
		os.Exit(1)
	}

	pool := worker.NewPool(queue, store, 5, nodeID, limiter, breaker, tenantID) // 5 concurrent workers
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

	grpcSrv := grpc.NewServer(grpc.UnaryInterceptor(grpcserver.TenantInterceptor))
	kairospb.RegisterKairosServiceServer(grpcSrv, grpcserver.New(queue, logger))

	go func() {
		var lc net.ListenConfig
		lis, err := lc.Listen(ctx, "tcp", ":9091")
		if err != nil {
			logger.Error("failed to listen for grpc", slog.Any("error", err))
			return
		}
		logger.Info("grpc server listening", slog.String("addr", ":9090"))
		if err := grpcSrv.Serve(lis); err != nil {
			logger.Error("grpc server error", slog.Any("error", err))
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
