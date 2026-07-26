// Command seedrecurring registers an example recurring job definition.
package main

import (
	"context"
	"encoding/json"
	"os"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/harshalvk/kairos/internal/scheduler"
)

func main() {
	ctx := context.Background()
	pgDSN := os.Getenv("POSTGRES_DSN")
	if pgDSN == "" {
		// #nosec G101 -- local development default only, not a real
		// credential. alays set POSTGRES_DSN in any non-local env
		pgDSN = "postgres://kairos:kairos@localhost:5432/kairos"
	}
	db, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	recurringStore := scheduler.NewStore(db)
	payload, err := json.Marshal(map[string]string{"to": "digest@example.com"})
	if err != nil {
		panic(err)
	}

	rj := &scheduler.RecurringJob{
		ID:          uuid.NewString(),
		Name:        "daily-digest-email",
		JobType:     "send_email",
		Payload:     payload,
		CronExpr:    "0 */10 * * * *", // every 10 seconds, for testing
		MaxAttempts: 3,
		Enabled:     true,
	}

	if err := recurringStore.Create(ctx, rj); err != nil {
		panic(err)
	}
	println("seeded recurring job:", rj.Name)
}
