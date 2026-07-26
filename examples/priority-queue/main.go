// Example priority-queue demonstrates that high-priority jobs are always
// dequeued before default/low priority ones, regardless of enqueue order.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/queue"
)

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	q := queue.New(rdb)

	payload, err := json.Marshal(map[string]string{"task": "demo"})
	if err != nil {
		panic(err)
	}

	// Enqueue low and default priority first, high last — high should
	// still come out of the queue first.
	low := job.NewWithPriority("cleanup", payload, 3, job.PriorityLow)
	def := job.NewWithPriority("report", payload, 3, job.PriorityDefault)
	high := job.NewWithPriority("password_reset", payload, 3, job.PriorityHigh)

	for _, j := range []*job.Job{low, def, high} {
		if err := q.Enqueue(ctx, j); err != nil {
			panic(err)
		}
		fmt.Printf("enqueued %-8s priority=%s\n", j.Type, j.Priority)
	}

	fmt.Println("\ndequeue order:")
	for i := 0; i < 3; i++ {
		got, err := q.Dequeue(ctx, 2*time.Second)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%d. %-8s priority=%s\n", i+1, got.Type, got.Priority)
	}
}
