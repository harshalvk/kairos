// Example job-dependencies demonstrates a DAG: a "notify" job only
// becomes runnable after its "resize_image" dependency completes.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/queue"
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

	upstream := job.New("resize_image", payload, 3)
	downstream := job.NewWithDependencies("notify_user", payload, 3, []string{upstream.ID})

	if enqErr := q.Enqueue(ctx, upstream); enqErr != nil {
		panic(enqErr)
	}
	if enqErr := q.EnqueueWithDependencies(ctx, downstream); enqErr != nil {
		panic(enqErr)
	}
	fmt.Println("enqueued upstream:", upstream.ID)
	fmt.Println("enqueued downstream (waiting on upstream):", downstream.ID)

	// Confirm downstream is NOT runnable yet.
	_, deqErr := q.Dequeue(ctx, 1*time.Second)
	fmt.Println("pending queue before resolution: empty =", deqErr != nil)

	// Simulate upstream completing.
	if resolveErr := q.ResolveDependents(ctx, upstream.ID); resolveErr != nil {
		panic(resolveErr)
	}
	fmt.Println("upstream resolved, downstream should now be runnable")

	got, err := q.Dequeue(ctx, 2*time.Second)
	if err != nil {
		panic(err)
	}
	fmt.Println("dequeued:", got.Type, "id:", got.ID)
}
