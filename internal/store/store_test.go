package store_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/url"
	"path/filepath"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file" // registers the "file" source driver
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver for pgx
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/store"
)

func migrationsSourceURL(t *testing.T) string {
	t.Helper()
	absPath, err := filepath.Abs("../../migrations")
	require.NoError(t, err)

	// On Windows, filepath.Abs returns a backslash path with a drive
	// letter (e.g. D:\foo\bar). Concatenating "file://" + that path
	// misparses the drive letter as a hostname with an invalid port.
	// Building the URL via net/url with forward-slashed path produces
	// the correct file:///D:/foo/bar form on any platform.
	u := url.URL{Scheme: "file", Path: filepath.ToSlash(absPath)}
	return u.String()
}

func setupPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("kairos"),
		tcpostgres.WithUsername("kairos"),
		tcpostgres.WithPassword("kairos"),
		tcpostgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.Terminate(ctx)) })

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	// golang-migrate needs a database/sql connection (via pgx's stdlib
	// driver), separate from the pgxpool.Pool the actual store code
	// uses — migrate doesn't speak pgx's native pool interface directly.
	sqlDB, err := sql.Open("pgx", connStr)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

	driver, err := postgres.WithInstance(sqlDB, &postgres.Config{})
	require.NoError(t, err)

	m, err := migrate.NewWithDatabaseInstance(migrationsSourceURL(t), "postgres", driver)
	require.NoError(t, err)
	if newDbInsterr := m.Up(); newDbInsterr != nil && err != migrate.ErrNoChange {
		require.NoError(t, err)
	}

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

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
