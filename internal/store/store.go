// Package store persists job lifecycle history to Postgres.
package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/tenant"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store persists job lifecycle history to Postgres. This is separate from
// Queue (Redis) on purpose — Queue answers "what needs to run next", Store
// answers "what happened, historically".
type Store struct {
	db *pgxpool.Pool
}

// NewStore creates a Store backed by the given Postgres connection pool.
func NewStore(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

// RecordCreated inserts a new row when a job is first created.
func (s *Store) RecordCreated(ctx context.Context, j *job.Job) error {
	_, err := s.db.Exec(ctx, `
		 INSERT INTO job_history (id, tenant_id, type, payload, status, attempts, max_attempts, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (id) DO NOTHING
	`, j.ID, tenant.FromContext(ctx), j.Type, j.Payload, j.Status, j.Attempts, j.MaxAttempts, j.CreatedAt)

	if err != nil {
		return fmt.Errorf("record created %w", err)
	}

	return nil
}

// RecordStatus updates a job's status, attempts, and last error — called
// after every completion, failure, retry, or dead-letter.
func (s *Store) RecordStatus(ctx context.Context, j *job.Job) error {
	_, err := s.db.Exec(ctx, `
		UPDATE job_history
		SET status = $2, attempts = $3, last_error = $4, result = $5::jsonb, updated_at = now()
		WHERE id = $1 AND tenant_id = $6
	`, j.ID, j.Status, j.Attempts, j.LastError, nullIfEmpty(j.Result), tenant.FromContext(ctx))
	if err != nil {
		return fmt.Errorf("record status: %w", err)
	}
	return nil
}

// nullIfEmpty converts an empty json.RawMessage to nil, so an unset
// Result stores as SQL NULL rather than an empty string that would
// fail the ::jsonb cast (empty string is not valid JSON).
func nullIfEmpty(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	return raw
}

// GetResult retrieves the result of a completed job by ID, scoped to
// the current tenant. Returns nil result (no error) if the job exists
// but has no result set, or if it hasn't completed yet.
func (s *Store) GetResult(ctx context.Context, jobID string) (json.RawMessage, error) {
	var result json.RawMessage
	err := s.db.QueryRow(ctx, `
		SELECT result FROM job_history WHERE id = $1 AND tenant_id = $2
	`, jobID, tenant.FromContext(ctx)).Scan(&result)
	if err != nil {
		return nil, fmt.Errorf("get result: %w", err)
	}
	return result, nil
}
