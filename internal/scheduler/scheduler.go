// Package scheduler defines recurring job schedules (cron-style) and
// persists them to postgres so they survive proces restarts
package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/harshalvk/kairos/internal/tenant"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RecurringJob defines a job type to be enqueued on a cron schedule
type RecurringJob struct {
	ID          string          `json:"id"`
	TenantID    string          `json:"tenant_id"`
	Name        string          `json:"name"`
	JobType     string          `json:"job_type"`
	Payload     json.RawMessage `json:"payload"`
	CronExpr    string          `json:"cron_expr"`
	MaxAttempts int             `json:"max_attempts"`
	Enabled     bool            `json:"enabled"`
	LastRunAt   *time.Time      `json:"last_run_at,omitempty"`
	NextRunAt   *time.Time      `json:"next_run_at,omitempty"`
}

// Store persists recurring job definitions to Postgres
type Store struct {
	db *pgxpool.Pool
}

// NewStore creates a scheduler Store backed by the given pool
func NewStore(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

// Create registers a new recurring job definition
func (s *Store) Create(ctx context.Context, rj *RecurringJob) error {
	if rj.TenantID == "" {
		rj.TenantID = tenant.DefaultTenant
	}
	_, err := s.db.Exec(ctx, `
		INSERT INTO recurring_jobs (id, tenant_id, name, job_type, payload, cron_expr, max_attempts, enabled) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, rj.ID, rj.TenantID, rj.Name, rj.JobType, rj.Payload, rj.CronExpr, rj.MaxAttempts, rj.Enabled)
	if err != nil {
		return fmt.Errorf("create recurring job: %w", err)
	}

	return nil
}

// ListEnabled returns all enabled recurring job definitions
func (s *Store) ListEnabled(ctx context.Context) ([]*RecurringJob, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id, tenant_id, name, job_type, payload, cron_expr, max_attempts, enabled, last_run_at, tenant_id FROM recurring_jobs WHERE enabled = true
	`)
	if err != nil {
		return nil, fmt.Errorf("list enabled recurring jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*RecurringJob
	for rows.Next() {
		var rj RecurringJob
		if err := rows.Scan(&rj.ID, &rj.TenantID, &rj.Name, &rj.JobType, &rj.Payload, &rj.CronExpr, &rj.MaxAttempts, &rj.Enabled, &rj.LastRunAt); err != nil {
			return nil, fmt.Errorf("scan recurring job: %w", err)
		}
		jobs = append(jobs, &rj)
	}
	return jobs, rows.Err()
}

// RecordRun updates the last-run timestamp after a recurring job fires
func (s *Store) RecordRun(ctx context.Context, id string, runAt time.Time) error {
	_, err := s.db.Exec(ctx, `
		UPDATE recurring_jobs SET last_run_at = $2 WHERE id = $1
	`, id, runAt)
	if err != nil {
		return fmt.Errorf("record recurring job run: %w", err)
	}
	return nil
}
