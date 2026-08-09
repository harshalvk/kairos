// Package webhook delivers job lifecycle notifications to external http
// endpoints, decoupled from job processing itself via a dedicated redis
// delivery queue and dispatcher
package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/harshalvk/kairos/internal/job"
	"github.com/redis/go-redis/v9"
)

const deliveryQueueKey = "kairos:webhook:deliveries"

// Event is the payload delivered to a webhook url
type Event struct {
	JobID     string          `json:"job_id"`
	JobType   string          `json:"job_type"`
	Event     string          `json:"event"`
	Status    job.Status      `json:"status"`
	Attempts  int             `json:"attempts"`
	Result    json.RawMessage `json:"result,omitempty"`
	LastError string          `json:"last_error,omitempty"`
	FiredAt   time.Time       `json:"fired_at"`
}

type delivery struct {
	URL     string `json:"url"`
	Event   Event  `json:"event"`
	Attempt int    `json:"attempt"`
}

// Dispatcher enqueues webhook deliveries and, when Run it called,
// processes them with its own bounded retry
type Dispatcher struct {
	rdb    *redis.Client
	client *http.Client
	logger *slog.Logger
}

// New creates a Dispatcher.
func New(rdb *redis.Client, logger *slog.Logger) *Dispatcher {
	return &Dispatcher{
		rdb:    rdb,
		client: &http.Client{Timeout: 10 * time.Second},
		logger: logger,
	}
}

// Notify checks j.Webhook and, if it's configured for eventName,
// enqueues a delivery; Cheap and non-blocking -- the actual http call
// happens later, in Run
func (d *Dispatcher) Notify(ctx context.Context, j *job.Job, eventName string) error {
	if j.Webhook == nil {
		return nil
	}
	wants := false
	for _, e := range j.Webhook.Events {
		if e == eventName {
			wants = true
			break
		}
	}
	if !wants {
		return nil
	}

	del := delivery{
		URL: j.Webhook.URL,
		Event: Event{
			JobID:     j.ID,
			JobType:   j.Type,
			Event:     eventName,
			Status:    j.Status,
			Attempts:  j.Attempts,
			Result:    j.Result,
			LastError: j.LastError,
			FiredAt:   time.Now(),
		},
	}
	data, err := json.Marshal(del)
	if err != nil {
		return fmt.Errorf("marshal webhook delivery: %w", err)
	}
	return d.rdb.LPush(ctx, deliveryQueueKey, data).Err()
}

// Run processes webhook deliveries until ctx is cancelled - a small,
// dedicated consumer loop, deliberately separate from job processing
func (d *Dispatcher) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		result, err := d.rdb.BRPop(ctx, 5*time.Second, deliveryQueueKey).Result()
		if err != nil {
			continue
		}

		var del delivery
		if err := json.Unmarshal([]byte(result[1]), &del); err != nil {
			d.logger.Error("failed to unmarshal webhook delivery", slog.Any("error", err))
			continue
		}

		if err := d.deliver(ctx, del); err != nil {
			del.Attempt++
			if del.Attempt >= 5 {
				d.logger.Error("webhook delivery permanently failed", slog.String("url", del.URL), slog.String("job_id", del.Event.JobID), slog.Any("error", err))
				continue
			}
			d.logger.Warn("webhook delivery failed, retrying", slog.String("url", del.URL), slog.Int("attempt", del.Attempt), slog.Any("error", err))
			data, marshalErr := json.Marshal(del)
			if marshalErr != nil {
				d.logger.Error("failed to marshal webhook delivery for retry, droping", slog.Any("error", marshalErr))
				continue
			}
			// simple backoff: re-push after a short delay via the caller's
			// own next pool cycle rather than a separate delayed queue -
			// acceptable given webhook delivery has a much shorter,
			// coarser retry budget than job processing itself
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(del.Attempt) * 2 * time.Second):
				if err := d.rdb.LPush(ctx, deliveryQueueKey, data).Err(); err != nil {
					d.logger.Error("failed to re-enqueue webhook delivery", slog.Any("error", err))
				}
			}
		}
	}
}

func (d *Dispatcher) deliver(ctx context.Context, del delivery) error {
	body, err := json.Marshal(del.Event)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, del.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Kairos-Event", del.Event.Event)

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			d.logger.Warn("failed to close webhook response body", slog.Any("error", closeErr))
		}
	}()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("webhook endpoint returned %d", resp.StatusCode)
	}
	return nil
}
