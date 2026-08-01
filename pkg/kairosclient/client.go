// Package kairosclient provides a Go client sdk for the kairos gRPC
// service, for use by external services that need to enqueue or inspect
// jobs without importing kairos's internal packages
package kairosclient

import (
	"context"
	"fmt"
	"math"

	"github.com/harshalvk/kairos/pkg/kairospb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func clampInt32(n int) int32 {
	if n > math.MaxInt32 {
		return math.MaxInt32
	}
	// #nosec G115 -- n is bounds-checked above; this conversion cannot
	// overflow. gosec's static analysis doesn't trace the preceding
	// guard, so this is a false positive, not a suppressed real issue.
	return int32(n)
}

// Client wraps a gRPC connection to a Kairos server
type Client struct {
	conn *grpc.ClientConn
	rpc  kairospb.KairosServiceClient
}

// Connect dials a Kairos gRPC server at addr (e.g. "localhost:9090")
// the returned client must be closed via close when no longer needed
func Connect(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connect to kairos: %w", err)
	}
	return &Client{conn: conn, rpc: kairospb.NewKairosServiceClient(conn)}, nil
}

// Close closes the underlying gRPC connection
func (c *Client) Close() error {
	return c.conn.Close()
}

// EnqueueOptions configures an Enqueue call
type EnqueueOptions struct {
	MaxAttempts    int
	Priority       kairospb.Priority
	IdempotencyKey string
}

// Enqueue submits a new job. Returns the assigned job ID and whether it
// was actually enqueued (false only when IdempotencyKey was set and
// mateched an existing, unexpired claim)
func (c *Client) Enqueue(ctx context.Context, jobType string, payload []byte, opts EnqueueOptions) (jobID string, enqueued bool, err error) {
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 3
	}
	resp, err := c.rpc.Enqueue(ctx, &kairospb.EnqueueRequest{
		Type:           jobType,
		Payload:        payload,
		MaxAttempts:    clampInt32(opts.MaxAttempts),
		Priority:       opts.Priority,
		IdempotencyKey: opts.IdempotencyKey,
	})
	if err != nil {
		return "", false, fmt.Errorf("enqueue: %w", err)
	}
	return resp.GetJobId(), resp.GetEnqueued(), nil
}

// QueueDepth returns the current pending job count per priority level
func (c *Client) QueueDepth(ctx context.Context) (map[string]int64, error) {
	resp, err := c.rpc.GetQueueDepth(ctx, &kairospb.GetQueueDepthRequest{})
	if err != nil {
		return nil, fmt.Errorf("get queue depth: %w", err)
	}
	return resp.GetDepthByPriority(), nil
}

// ListDeadLetter returns up to limit dead-lettered jobs
func (c *Client) ListDeadLetter(ctx context.Context, limit int64) ([]*kairospb.Job, error) {
	resp, err := c.rpc.ListDeadLetter(ctx, &kairospb.ListDeadLetterRequest{Limit: limit})
	if err != nil {
		return nil, fmt.Errorf("list dead letter: %w", err)
	}
	return resp.GetJobs(), nil
}

// RequeueDeadLetter requeues a dead-lettered job by ID
func (c *Client) RequeueDeadLetter(ctx context.Context, jobID string) error {
	_, err := c.rpc.RequeueDeadLetter(ctx, &kairospb.RequeueDeadLetterRequest{JobId: jobID})
	if err != nil {
		return fmt.Errorf("requeue dead letter: %w", err)
	}
	return nil
}
