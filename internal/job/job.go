// Package job defines the core Job domain model shared across the queue,
// worker, and store packages.
package job

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Status represents the current lifecycle state of a Job.
type Status string

// Priority controls dequeu order: workers check high, then default, then
// low in that order before blocking
type Priority string

// Possible values for Priority
const (
	PriorityHigh    Priority = "high"
	PriorityDefault Priority = "default"
	PriorityLow     Priority = "low"
)

// Possible values for Status.
const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
	StatusDeadLetter Status = "dead_letter"
)

// Job represents a single unit of work to be processed by a worker.
type Job struct {
	ID             string          `json:"id"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Result         json.RawMessage `json:"result,omitempty"`
	Status         Status          `json:"status"`
	Priority       Priority        `json:"priority"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	CreatedAt      time.Time       `json:"created_at"`
	RunAt          time.Time       `json:"run_at"`
	LastError      string          `json:"last_error,omitempty"`
	DependsOn      []string        `json:"depends_on,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

// New creates a new Job with a generated UUID and pending status.
func New(jobType string, payload json.RawMessage, maxAttempts int) *Job {
	return &Job{
		ID:          uuid.NewString(),
		Type:        jobType,
		Payload:     payload,
		Status:      StatusPending,
		Priority:    PriorityDefault,
		Attempts:    0,
		MaxAttempts: maxAttempts,
		CreatedAt:   time.Now(),
		RunAt:       time.Now(),
	}
}

// NewWithPriority creates a new Job with an explicit priority.
func NewWithPriority(jobType string, payload json.RawMessage, maxAttempts int, priority Priority) *Job {
	j := New(jobType, payload, maxAttempts)
	j.Priority = priority
	return j
}

// NewWithDependencies creates a new Job that only becomes eligible to run
// once every job id in dependsOn has completed successfully
func NewWithDependencies(jobType string, payload json.RawMessage, maxAttempts int, dependsOn []string) *Job {
	j := New(jobType, payload, maxAttempts)
	j.DependsOn = dependsOn
	return j
}

// NewWithIdempotencyKey creates a new job tagged with a caller-supplied
// idempotency key; enqueuing two jobs with the same key (of the same type)
// results in only the first one actually running
func NewWithIdempotencyKey(jobType string, payload json.RawMessage, maxAttempts int, key string) *Job {
	j := New(jobType, payload, maxAttempts)
	j.IdempotencyKey = key
	return j
}

// SetResult attaches a result to the job, to be persisted once it
// completes successfully. v is json-marshaled
func (j *Job) SetResult(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal result: %w", err)
	}
	j.Result = data
	return nil
}
