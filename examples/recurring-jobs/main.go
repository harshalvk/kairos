// Example recurring-jobs demonstrates registering a cron-style recurring
// job definition, which cmd/cron picks up and fires on schedule.
package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/harshalvk/kairos/internal/scheduler"
)

func main() {
	ctx := context.Background()
	db, err := pgxpool.New(ctx, "postgres://kairos:kairos@localhost:5432/kairos")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	store := scheduler.NewStore(db)
	payload, err := json.Marshal(map[string]string{"to": "digest@example.com"})
	if err != nil {
		panic(err)
	}

	rj := &scheduler.RecurringJob{
		ID:          uuid.NewString(),
		Name:        "daily-digest-email",
		JobType:     "send_email",
		Payload:     payload,
		CronExpr:    "0 0 9 * * *", // 9am daily (6-field: seconds first)
		MaxAttempts: 3,
		Enabled:     true,
	}

	if err := store.Create(ctx, rj); err != nil {
		panic(err)
	}
	fmt.Println("registered recurring job:", rj.Name, "schedule:", rj.CronExpr)
	fmt.Println("run `go run ./cmd/cron` to start firing it")
}
