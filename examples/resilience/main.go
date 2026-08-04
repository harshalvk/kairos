// Example resilience demonstrates idempotency keys (duplicate enqueue
// attempts are skipped), rate limiting (token bucket per job type), and
// the circuit breaker (opens after repeated failures, then half-opens
// for a trial).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/harshalvk/kairos/internal/circuitbreaker"
	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/queue"
	"github.com/harshalvk/kairos/internal/ratelimit"
	"github.com/harshalvk/kairos/internal/tenant"
)

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	registry := tenant.NewRegistry(rdb)
	q := queue.New(rdb, registry)
	payload, err := json.Marshal(map[string]string{"task": "demo"})
	if err != nil {
		panic(err)
	}

	// --- Idempotency ---
	fmt.Println("--- idempotency ---")
	first := job.NewWithIdempotencyKey("charge_card", payload, 3, "order-42")
	second := job.NewWithIdempotencyKey("charge_card", payload, 3, "order-42")

	ok1, err := q.EnqueueIdempotent(ctx, first, time.Hour)
	if err != nil {
		panic(err)
	}
	ok2, err := q.EnqueueIdempotent(ctx, second, time.Hour)
	if err != nil {
		panic(err)
	}
	fmt.Println("first enqueue succeeded:", ok1)
	fmt.Println("duplicate enqueue succeeded:", ok2, "(should be false)")

	// --- Rate limiting ---
	fmt.Println("\n--- rate limiting ---")
	limiter := ratelimit.New()
	limiter.SetLimit("send_email", 2, 1) // 2/sec, burst 1

	start := time.Now()
	if waitErr := limiter.Wait(ctx, "send_email"); waitErr != nil { // consumes burst token, instant
		panic(waitErr)
	}
	if waitErr := limiter.Wait(ctx, "send_email"); waitErr != nil { // waits ~500ms for next token
		panic(waitErr)
	}
	fmt.Printf("two rate-limited calls took %s (second one throttled)\n", time.Since(start).Round(time.Millisecond))

	// --- Circuit breaker ---
	fmt.Println("\n--- circuit breaker ---")
	cb := circuitbreaker.New(3, 500*time.Millisecond)
	simulateFailure := func() error { return errors.New("downstream is down") }

	for i := 1; i <= 3; i++ {
		if cb.Allow("flaky_api") {
			err := simulateFailure()
			cb.RecordFailure("flaky_api")
			fmt.Printf("attempt %d: allowed, failed (%v), state=%s\n", i, err, cb.StateOf("flaky_api"))
		}
	}
	fmt.Println("circuit is now open — next call rejected without attempting:")
	fmt.Println("allowed:", cb.Allow("flaky_api"))

	time.Sleep(600 * time.Millisecond)
	fmt.Println("after cooldown, one trial allowed, state=", cb.StateOf("flaky_api"))
	fmt.Println("allowed:", cb.Allow("flaky_api"), "state=", cb.StateOf("flaky_api"))
}
