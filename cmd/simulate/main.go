// Command simulate seeds the queue with a realistic mix of jobs across
// priorities and types — including some pre-failed dead-lettered jobs —
// purely so there's real data to look at in the TUI or dashboards
// without needing to run a full application against Kairos.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/queue"
	"github.com/harshalvk/kairos/internal/tenant"
)

type jobTemplate struct {
	jobType  string
	priority job.Priority
	payload  func() any
}

var templates = []jobTemplate{
	{"send_email", job.PriorityHigh, func() any {
		return map[string]string{"to": fmt.Sprintf("user%d@example.com", rand.Intn(9999)), "template": "password_reset"}
	}},
	{"send_email", job.PriorityDefault, func() any {
		return map[string]string{"to": fmt.Sprintf("user%d@example.com", rand.Intn(9999)), "template": "weekly_digest"}
	}},
	{"resize_image", job.PriorityDefault, func() any {
		return map[string]any{"image_id": rand.Intn(99999), "sizes": []string{"thumb", "medium", "large"}}
	}},
	{"generate_report", job.PriorityLow, func() any {
		return map[string]string{"report_type": "monthly_usage", "org_id": fmt.Sprintf("org_%d", rand.Intn(500))}
	}},
	{"webhook_delivery", job.PriorityHigh, func() any {
		return map[string]string{"url": "https://example.com/hooks/incoming", "event": "order.created"}
	}},
	{"sync_inventory", job.PriorityLow, func() any {
		return map[string]int{"warehouse_id": rand.Intn(20), "sku_count": rand.Intn(500)}
	}},
	{"charge_card", job.PriorityHigh, func() any {
		return map[string]any{"order_id": fmt.Sprintf("order_%d", rand.Intn(99999)), "amount_cents": 500 + rand.Intn(20000)}
	}},
	{"cleanup_tmp_files", job.PriorityLow, func() any {
		return map[string]string{"path": "/tmp/uploads"}
	}},
}

func main() {
	pendingCount := flag.Int("pending", 40, "number of jobs to enqueue into the pending queue")
	deadCount := flag.Int("dead", 8, "number of jobs to seed directly into the dead-letter queue")
	redisAddr := flag.String("redis", "localhost:6379", "redis address")
	flag.Parse()

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	registry := tenant.NewRegistry(rdb)
	q := queue.New(rdb, registry)
	ctx := context.Background()

	if err := rdb.Ping(ctx).Err(); err != nil {
		fmt.Println("failed to reach redis:", err)
		return
	}

	fmt.Printf("seeding %d pending jobs...\n", *pendingCount)
	for i := 0; i < *pendingCount; i++ {
		t := templates[rand.Intn(len(templates))]
		payload, err := json.Marshal(t.payload())
		if err != nil {
			log.Printf("marshal payload for %s: %v", t.jobType, err)
			continue // or return, depending on the loop context
		}
		j := job.NewWithPriority(t.jobType, payload, 3, t.priority)
		if err := q.Enqueue(ctx, j); err != nil {
			fmt.Println("enqueue failed:", err)
			continue
		}
	}

	fmt.Printf("seeding %d dead-lettered jobs...\n", *deadCount)
	deadReasons := []string{
		"connection refused: downstream email provider unreachable",
		"context deadline exceeded",
		"invalid payload: missing required field 'to'",
		"429 too many requests from upstream API",
		"panic recovered: nil pointer in handler",
	}
	for i := 0; i < *deadCount; i++ {
		t := templates[rand.Intn(len(templates))]
		payload, err := json.Marshal(t.payload())
		if err != nil {
			log.Printf("marshal payload for %s: %v", t.jobType, err)
			continue
		}
		j := job.NewWithPriority(t.jobType, payload, 3, t.priority)
		j.Attempts = 3
		j.LastError = deadReasons[rand.Intn(len(deadReasons))]
		if err := q.MoveToDeadLetter(ctx, j); err != nil {
			fmt.Println("dead-letter seed failed:", err)
			continue
		}
	}

	// A short burst of delayed/retry-scheduled jobs, so the delayed set
	// isn't empty either.
	fmt.Println("seeding a few delayed jobs...")
	for i := 0; i < 5; i++ {
		t := templates[rand.Intn(len(templates))]
		payload, err := json.Marshal(t.payload())
		if err != nil {
			log.Printf("marshal payload for %s: %v", t.jobType, err)
			continue
		}
		j := job.NewWithPriority(t.jobType, payload, 3, t.priority)
		runAt := time.Now().Add(time.Duration(rand.Intn(120)) * time.Second)
		if err := q.EnqueueDelayed(ctx, j, runAt); err != nil {
			fmt.Println("delayed seed failed:", err)
		}
	}

	fmt.Println("done. run `go run ./cmd/tui` (with the admin API up) to see it.")
}
