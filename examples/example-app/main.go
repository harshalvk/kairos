// Command example-app demonstrates the pkg/kairos ergonomic client:
// registering a handler and enqueueing a job with functional options.
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/harshalvk/kairos/pkg/kairos"
)

type WelcomeEmail struct {
	To   string `json:"to"`
	Name string `json:"name"`
}

func main() {
	client, err := kairos.New(
		kairos.WithRedisAddr("localhost:6379"),
		kairos.WithPostgresDSN("postgres://kairos:kairos@localhost:5432/kairos"),
		kairos.WithConcurrency(10),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("close client: %v", err)
		}
	}()

	client.Handle("welcome_email", func(_ context.Context, j kairos.Job) error {
		var p WelcomeEmail
		if err := j.Bind(&p); err != nil {
			return err
		}
		log.Printf("sending welcome email to %s (%s)", p.Name, p.To)
		return nil
	}, kairos.MaxAttempts(5), kairos.RateLimit(10, 20))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if _, err := client.Enqueue(ctx, "welcome_email", WelcomeEmail{To: "new@user.com", Name: "Ada"}, kairos.Priority(kairos.High)); err != nil {
		log.Printf("enqueue failed: %v", err)
	}

	client.Run(ctx, 30*time.Second)
}
