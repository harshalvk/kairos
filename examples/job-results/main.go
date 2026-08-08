// Example job-results demonstrates a handler computing a value and
// making it retrievable after the job completes.
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/harshalvk/kairos/pkg/kairos"
)

type SumRequest struct{ A, B int }
type SumResult struct {
	Total int `json:"total"`
}

func main() {
	client, err := kairos.New(kairos.WithPostgresDSN("postgres://kairos:kairos@localhost:5432/kairos"))
	if err != nil {
		panic(err)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			fmt.Println("close client:", closeErr)
		}
	}()

	client.Handle("sum", func(_ context.Context, j kairos.Job) error {
		var req SumRequest
		if bindErr := j.Bind(&req); bindErr != nil {
			return bindErr
		}
		return j.SetResult(SumResult{Total: req.A + req.B})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobID, err := client.Enqueue(ctx, "sum", SumRequest{A: 4, B: 5})
	if err != nil {
		panic(err)
	}
	go client.Run(ctx, 10*time.Second)

	time.Sleep(1 * time.Second)
	var result SumResult
	if err := client.Result(ctx, jobID, &result); err != nil {
		panic(err)
	}
	fmt.Println("4 + 5 =", result.Total)
}
