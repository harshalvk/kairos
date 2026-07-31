// Example streams-vs-lists demonstrates the crash-recovery capability
// Redis Streams provide that internal/queue's list-based implementation
// (ADR 0001) does not: a message delivered to a consumer that never
// acknowledges it can be reclaimed by another consumer via XAutoClaim.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/streamqueue"
)

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	sq, err := streamqueue.New(ctx, rdb, "consumer-a")
	if err != nil {
		panic(err)
	}

	payload, err := json.Marshal(map[string]string{
		"task": "demo",
	})
	if err != nil {
		panic(err)
	}

	j := job.New("demo_task", payload, 3)

	err = sq.Enqueue(ctx, j)
	if err != nil {
		panic(err)
	}

	fmt.Println("enqueued:", j.ID)

	// Simulate consumer-a receiving the message but crashing before Ack.
	got, msgID, err := sq.Dequeue(ctx, 2*time.Second)
	if err != nil {
		panic(err)
	}

	fmt.Printf(
		"consumer-a received job %s (message %s) — simulating crash, never acking\n",
		got.ID,
		msgID,
	)

	pending, err := sq.PendingCount(ctx)
	if err != nil {
		panic(err)
	}

	fmt.Println("pending (delivered, unacked) count:", pending)

	// A second consumer reclaims it after a short idle window.
	sqB, err := streamqueue.New(ctx, rdb, "consumer-b")
	if err != nil {
		panic(err)
	}

	time.Sleep(1 * time.Second)

	jobs, ids, err := sqB.ClaimStale(ctx, 500*time.Millisecond, 10)
	if err != nil {
		panic(err)
	}

	for i, claimedJob := range jobs {
		fmt.Printf(
			"consumer-b reclaimed job %s (message %s) — processing and acking\n",
			claimedJob.ID,
			ids[i],
		)

		err = sqB.Ack(ctx, ids[i])
		if err != nil {
			panic(err)
		}
	}

	pending, err = sq.PendingCount(ctx)
	if err != nil {
		panic(err)
	}

	fmt.Println("pending after reclaim+ack:", pending, "(should be 0)")
}
