// Example basic demonstrates the core enqueue -> worker -> retry loop:
// a job that fails twice before succeeding, showing backoff retries in
// the logs.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/harshalvk/kairos/internal/circuitbreaker"
	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/queue"
	"github.com/harshalvk/kairos/internal/ratelimit"
	"github.com/harshalvk/kairos/internal/tenant"
	"github.com/harshalvk/kairos/internal/worker"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	q := queue.New(rdb)

	tenantID := os.Getenv("TENANT_ID")
	if tenantID == "" {
		tenantID = tenant.DefaultTenant
	}
	if err := tenant.Validate(tenantID); err != nil {
		slog.Error("invalid TENANT_ID", slog.Any("error", err))
		os.Exit(1)
	}

	attempts := 0
	pool := worker.NewPool(q, nil, 2, "basic-example", ratelimit.New(), circuitbreaker.New(5, time.Minute), tenantID)
	pool.RegisterHandler("flaky_task", func(_ context.Context, j *job.Job) error {
		attempts++
		if attempts < 3 {
			return errors.New("simulated transient failure")
		}
		fmt.Printf("job %s succeeded on attempt %d\n", j.ID, attempts)
		return nil
	})

	payload, err := json.Marshal(map[string]string{"task": "demo"})
	if err != nil {
		log.Fatalf("failed to marshal payload: %v", err)
	}
	j := job.New("flaky_task", payload, 5)
	if err := q.Enqueue(ctx, j); err != nil {
		panic(err)
	}
	fmt.Println("enqueued:", j.ID)

	pool.Start(ctx, 10*time.Second)
}
