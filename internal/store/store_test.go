package store_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

func setupPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("kairos"),
		tcpostgres.WithUsername("kairos"),
		tcpostgres.WithPassword("kairos"),
		tcpostgres.BasicWaitStrategies())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, container.Terminate(ctx))
	})

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	_, err = pool.Exec(ctx, `
CREATE TABLE job_history (
id UUID PRIMARY KEY,
tenant_id       TEXT NOT NULL DEFAULT 'default',
type TEXT NOT NULL,
payload JSONB NOT NULL,
status TEXT NOT NULL,
attempts INT NOT NULL DEFAULT 0,
max_attempts INT NOT NULL,
last_error TEXT,
created_at TIMESTAMPTZ NOT NULL,
updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
`)
	require.NoError(t, err)

	return pool
}

func TestRecordCreatedAndStatus(t *testing.T) {
	pool := setupPostgres(t)
	s := store.NewStore(pool)
	ctx := context.Background()

	payload, err := json.Marshal(map[string]string{"to": "test@example.com"})
	require.NoError(t, err)
	j := job.New("send_email", payload, 3)

	require.NoError(t, s.RecordCreated(ctx, j))

	j.Status = job.StatusCompleted
	require.NoError(t, s.RecordStatus(ctx, j))

	var gotStatus string
	err = pool.QueryRow(ctx, "SELECT status FROM job_history WHERE id = $1", j.ID).Scan(&gotStatus)
	require.NoError(t, err)
	assert.Equal(t, string(job.StatusCompleted), gotStatus)
}

func TestRecordCreated_IgnoresDuplicateID(t *testing.T) {
	pool := setupPostgres(t)
	s := store.NewStore(pool)
	ctx := context.Background()

	payload, err := json.Marshal(map[string]string{"to": "test@example.com"})
	require.NoError(t, err)
	j := job.New("send_email", payload, 3)

	require.NoError(t, s.RecordCreated(ctx, j))
	require.NoError(t, s.RecordCreated(ctx, j)) // should not error, ON CONFLICT DO NOTHING

	var count int
	err = pool.QueryRow(ctx, "SELECT count(*) FROM job_history WHERE id = $1", j.ID).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}
