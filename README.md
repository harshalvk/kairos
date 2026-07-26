# Kairos

![visitors](https://visitor-badge.laobi.icu/badge?page_id=harshalvk.job-queue&left_text=visitors&left_color=%234f4f4f&right_color=%23c48312)
[![CI](https://github.com/harshalvk/kairos/actions/workflows/ci.yml/badge.svg)](https://github.com/harshalvk/Job-Queue/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/harshalvk/kairos)](https://goreportcard.com/report/github.com/harshalvk/kairos)
[![Go Reference](https://pkg.go.dev/badge/github.com/harshalvk/kairos.svg)](https://pkg.go.dev/github.com/harshalvk/kairos)
<img width="1408" height="319" alt="kairos logo" src="https://github.com/user-attachments/assets/a2d604bf-fae5-4cf9-bfd9-2205f4614dc4" />

A distributed job queue built from scratch in Go — a mini Sidekiq/Celery, without reaching for an off-the-shelf framework. The goal is to actually understand the primitives (worker pools, retries, dead-lettering, priority queues, dependency graphs, idempotency) rather than just importing a library that hides them.

## Why "Kairos"

In ancient Greek, *chronos* is clock time — sequential, measured. *Kairos* is the right, opportune moment for something to happen. That's really what this queue is about: not running things fast, but running each job at the moment it's actually meant to run — after dependencies resolve, once backoff has passed, ahead of lower-priority work when it matters. Kairos felt like the right name for a system whose whole job is figuring out the right moment.

## Why build this instead of using an existing library?

Libraries like Sidekiq, Celery, or Asynq solve this problem well — but using them skips past the actual mechanics:

- how does a worker pool avoid spawning unbounded goroutines?
- How do retries avoid hammering a failing dependency?
- How do you not lose jobs when a process crashes mid-retry?

This project builds each of those pieces manually, one at a time, with the reasoning behind each design decision documented alongside the code.

## Architecture
<img width="6447" height="3411" alt="image" src="https://github.com/user-attachments/assets/af480ba5-f471-4763-9b4a-2f93a3b0664a" />

- **Producer** enqueues jobs — plain, priority-tagged, dependency-linked, or idempotency-guarded.
- **Redis** holds all live state: three priority-ordered pending lists, a delayed sorted set for retries/scheduled jobs, a dead-letter list, a waiting hash for unresolved dependencies, and idempotency claim keys.
- **Scheduler** is a separate process whose only job is promoting due delayed jobs into pending.
- **Worker pool** dequeues (priority-first), executes handlers, and on outcome: records history to Postgres, updates metrics, and either resolves/cascades dependent jobs or schedules a retry.

## Project structure

```
kairos/
├── go.mod
├── internal/
│   ├── job/              # core Job domain model
│   ├── queue/             # Redis-backed queue: pending, dead-letter, delayed, dependencies, idempotency
│   ├── store/              # Postgres job history persistence
│   ├── metrics/            # Prometheus metrics
│   ├── worker/              # worker pool, retries, dead-lettering
│   ├── ratelimit/           # per-job-type token bucket rate limiting
│   ├── circuitbreaker/      # per-job-type circuit breaker
│   └── scheduler/           # recurring (cron) job definitions
├── cmd/
│   ├── producer/            # enqueue a test job
│   ├── worker/              # run the worker pool
│   ├── scheduler/           # promote due delayed/retry jobs
│   ├── cron/                # fire recurring jobs on schedule
│   ├── deadletter/          # inspect/requeue/purge dead-lettered jobs
│   └── seed-recurring/      # register example recurring job definitions
├── examples/                # runnable examples of Kairos capabilities
├── migrations/               # Postgres schema migrations
├── docs/adr/                 # architecture decision records
├── observability/            # Prometheus + Grafana provisioning
├── docker-compose.yml
├── Dockerfile
└── .goreleaser.yml
```

## Supported features

- **Priority queues** — high/default/low, dequeued in order via a single atomic multi-key `BRPOP`
- **Job dependencies (DAGs)** — jobs run only after declared dependencies complete; failures cascade to dependents
- **Idempotency keys** — duplicate enqueue attempts (same type + key) are skipped via atomic `SETNX`
- **Retries with backoff** — exponential backoff, capped, dead-lettered after `MaxAttempts`
- **Dead-letter queue** — inspect, requeue, or purge permanently-failed jobs
- **Durable delayed jobs** — retries and scheduled jobs survive worker restarts via a Redis sorted set
- **Recurring (cron) jobs** — Postgres-backed schedules, fired by a dedicated `cmd/cron` process
- **Rate limiting** — per-job-type token bucket, independent of worker concurrency
- **Circuit breaker** — per-job-type closed/open/half-open, stops hammering a failing dependency
- **Postgres persistence** — full job lifecycle history, queryable independently of live queue state
- **Metrics** — Prometheus counters/histogram/gauges; Grafana dashboard included
- **Graceful shutdown** — bounded drain of in-flight jobs on SIGTERM/SIGINT
- **Multi-node ready** — safe concurrent workers via `BRPOP`, no extra coordination needed

## Running it locally

```bash
# start redis + postgres
docker compose up -d

# run schema against postgres (copy + exec avoids psql needing to be installed locally)
docker cp schema.sql kairos-postgres:/schema.sql
docker exec -it kairos-postgres psql -U kairos -d kairos -f /schema.sql

# terminal 1: start the worker pool (serves metrics on :2112/metrics)
go run ./cmd/worker

# terminal 2: start the scheduler (promotes due delayed/retry jobs)
go run ./cmd/scheduler

# terminal 3: enqueue a test job
go run ./cmd/producer

# inspect dead-lettered jobs
go run ./cmd/deadletter -action=list
go run ./cmd/deadletter -action=requeue -id=<job-uuid>
go run ./cmd/deadletter -action=purge

# stop everything (keeps data)
docker compose down

# stop and wipe all data
docker compose down -v
```

## Requirements

- Go 1.21+
- Docker + Docker Compose (Redis + Postgres)
- github.com/redis/go-redis/v9
- github.com/google/uuid
- github.com/jackc/pgx/v5/pgxpool
- github.com/prometheus/client_golang
  > README.md is ai-generated

## Documentation

- [Contributing guide](CONTRIBUTING.md) — setup, commands, commit conventions
- [Architecture Decision Records](docs/adr/README.md) — the reasoning behind major design choices
