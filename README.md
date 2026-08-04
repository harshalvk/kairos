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

## Project structure

```
kairos/
├── go.mod
├── proto/kairos/v1/          # gRPC service definition
├── internal/
│   ├── job/                  # core Job domain model
│   ├── queue/                 # Redis-backed queue: pending, dead-letter, delayed, dependencies, idempotency, tenant registry
│   ├── store/                  # Postgres job history persistence
│   ├── metrics/                 # Prometheus metrics
│   ├── worker/                   # worker pool, retries, dead-lettering
│   ├── ratelimit/                 # per-job-type token bucket rate limiting
│   ├── circuitbreaker/             # per-job-type circuit breaker
│   ├── scheduler/                   # recurring (cron) job definitions
│   ├── streamqueue/                  # Redis Streams alternative (comparison, not primary path — ADR 0020)
│   ├── leaderelection/                # Redis-based distributed lock for the scheduler
│   ├── logging/                        # structured slog logger, context-propagated
│   ├── tracing/                         # OpenTelemetry tracing setup
│   ├── tenant/                           # multi-tenancy: context propagation + tenant registry
│   ├── config/                            # centralized environment configuration
│   ├── api/                                # admin HTTP API (chi router)
│   └── grpcserver/                          # gRPC service implementation
├── pkg/
│   ├── kairospb/              # generated protobuf/gRPC code
│   └── kairosclient/           # public Go client SDK
├── cmd/
│   ├── producer/               # enqueue a test job
│   ├── worker/                  # run the worker pool (serves :2112 metrics, :8080 admin API, :9090 gRPC)
│   ├── scheduler/                 # leader-elected: promotes due delayed/retry jobs across all tenants
│   ├── cron/                       # fires recurring jobs on schedule
│   ├── deadletter/                  # inspect/requeue/purge dead-lettered jobs
│   └── seed-recurring/               # register example recurring job definitions
├── examples/                  # runnable examples: basic, priority-queue, job-dependencies, resilience, streams-vs-lists, grpc-client
├── migrations/                 # Postgres schema migrations
├── docs/adr/                    # 24 architecture decision records
├── observability/                # Prometheus + Grafana provisioning
├── loadtest/                      # k6 load testing scripts
├── docker-compose.yml
├── Dockerfile
├── .goreleaser.yml
├── .releaserc.json               # semantic-release config
└── .go-arch-lint.yml              # package dependency boundary rules
```

## Supported features

**Core queue**
- Priority queues (high/default/low), dequeued atomically via multi-key `BRPOP`
- Job dependencies (DAGs) — jobs run only after declared dependencies complete; failures cascade
- Idempotency keys — duplicate enqueues (same type + key) skipped via atomic `SETNX`
- Retries with exponential backoff, dead-lettered after `MaxAttempts`
- Durable delayed jobs — survive worker restarts via a Redis sorted set
- Recurring (cron) jobs — Postgres-backed schedules, tenant-aware
- Redis Streams alternative implementation for comparison (`internal/streamqueue`, see ADR 0020)

**Resilience**
- Per-job-type rate limiting (token bucket)
- Per-job-type circuit breaker (closed/open/half-open)
- Leader-elected scheduler with automatic failover (Redis `SETNX` + TTL)
- Graceful shutdown — bounded drain of in-flight jobs

**Multi-tenancy**
- Context-propagated tenant identity across queue, store, worker, HTTP, and gRPC
- Redis keys and Postgres rows fully namespaced per tenant
- Auto-registering tenant registry — no manual provisioning step

**Interfaces**
- CLI (`cmd/producer`, `cmd/deadletter`)
- Admin HTTP API (`chi` router, `X-Tenant-ID` header)
- gRPC service + public Go client SDK (`pkg/kairosclient`)

**Observability**
- Structured JSON logging (`slog`), context-propagated
- OpenTelemetry tracing (nested spans, exported to Jaeger)
- Prometheus metrics + a pre-built Grafana dashboard

**Developer experience**
- golangci-lint, gosec, govulncheck, go-arch-lint (package boundary enforcement)
- Unit, integration (testcontainers), fuzz, and load (k6) tests
- Semantic-release — versioning driven entirely by Conventional Commits
- Devcontainer, Docker multi-stage builds, cross-platform releases via goreleaser
- 24 ADRs documenting every non-trivial decision

## Running it locally

```bash
docker compose up -d --build

# apply migrations in order (see "Known gaps" below)
docker cp migrations/0001_init.sql kairos-postgres:/0001_init.sql
docker exec -it kairos-postgres psql -U kairos -d kairos -f /0001_init.sql
docker cp migrations/0002_recurring_jobs.sql kairos-postgres:/0002_recurring_jobs.sql
docker exec -it kairos-postgres psql -U kairos -d kairos -f /0002_recurring_jobs.sql
docker cp migrations/0003_multi_tenancy.sql kairos-postgres:/0003_multi_tenancy.sql
docker exec -it kairos-postgres psql -U kairos -d kairos -f /0003_multi_tenancy.sql
```

## Known gaps

- **Migrations are applied manually, one file at a time** — no migration runner tracks which have been applied. Worth fixing with a proper tool (`golang-migrate`, `goose`) before this goes anywhere beyond local dev.
- **One worker pool serves exactly one tenant** — a tenant needs its own worker process(es) to have its jobs actually processed (ADR 0024).
- **CLI tools default to the `default` tenant** with no flag to target another one yet.
- **No authentication** on the admin HTTP API or gRPC service (ADR 0016, ADR 0021) — safe only on a trusted network today.
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
