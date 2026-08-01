// Example grpc-client demonstrates using the kairosclient SDK to enqueue
// a job from an external service without importing Kairos's internal
// packages.
package main

import (
	"context"
	"fmt"

	"github.com/harshalvk/kairos/pkg/kairosclient"
	"github.com/harshalvk/kairos/pkg/kairospb"
)

func main() {
	client, err := kairosclient.Connect("localhost:9090")
	if err != nil {
		panic(err)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			fmt.Println("failed to close client:", closeErr)
		}
	}()

	ctx := context.Background()
	jobID, enqueued, err := client.Enqueue(ctx, "send_email", []byte(`{"to":"you@example.com"}`), kairosclient.EnqueueOptions{
		Priority: kairospb.Priority_PRIORITY_HIGH,
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("enqueued:", enqueued, "job id:", jobID)

	depth, err := client.QueueDepth(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println("queue depth:", depth)
}
