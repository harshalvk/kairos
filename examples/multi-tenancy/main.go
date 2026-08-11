// Example multi-tenancy demonstrates that two tenants' jobs are fully
// isolated — enqueuing for one tenant never becomes visible to another,
// even against the same Redis instance.
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
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	registry := tenant.NewRegistry(rdb)
	q := queue.New(rdb, registry)

	ctxAcme := tenant.WithContext(context.Background(), "acme-corp")
	ctxGlobex := tenant.WithContext(context.Background(), "globex-inc")

	payload, err := json.Marshal(map[string]string{"to": "demo@example.com"})
	if err != nil {
		panic(err)
	}

	acmeJob := job.New("send_email", payload, 3)
	if enqueueErr := q.Enqueue(ctxAcme, acmeJob); enqueueErr != nil {
		panic(enqueueErr)
	}
	fmt.Println("enqueued for acme-corp:", acmeJob.ID)

	// globex-inc's queue is untouched — it never sees acme's job.
	_, err = q.Dequeue(ctxGlobex, []string{"send_email"}, 1*time.Second)
	fmt.Println("globex-inc queue empty:", err != nil)

	// acme-corp can dequeue its own job normally.
	got, err := q.Dequeue(ctxAcme, []string{"send_email"}, 2*time.Second)
	if err != nil {
		panic(err)
	}
	fmt.Println("acme-corp dequeued:", got.ID)
}
