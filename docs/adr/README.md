# Architecture Decision Records

Records of significant architectural decisions made in this project,
in Context → Decision → Consequences format.

| ADR | Title |
|-----|-------|
| [0001](0001-redis-list-over-streams-for-queue.md) | Redis list over Streams for the job queue |
| [0002](0002-fixed-size-worker-pool.md) | Fixed-size worker pool |
| [0003](0003-durable-delayed-jobs-via-sorted-set.md) | Durable delayed/retry jobs via sorted set |
| [0004](0004-separate-dead-letter-queue.md) | Separate dead-letter queue |
| [0005](0005-graceful-shutdown-with-bounded-timeout.md) | Graceful shutdown with bounded timeout |
| [0006](0006-multi-node-workers-require-no-code-changes.md) | Multi-node workers require no code changes |
| [0007](0007-dual-write-redis-and-postgres.md) | Dual-write to Redis and Postgres |
| [0008](0008-priority-queues-via-multiple-redis-lists.md) | Priority queues via multiple Redis lists |
| [0009](0009-job-dependencies-via-waiting-hash-and-reverse-index.md) | Job dependencies via a waiting hash |
| [0010](0010-idempotency-keys-via-setnx.md) | Idempotency keys in via Redis SETNX |
| [0011](0011-per-job-type-rate-limiting.md) | Per-job-type rate limiting via token buckets |
| [0012](docs/adr/0012-recurring-jobs-persisted-to-postgres.md) | Recurring jobs defined via cron expressions |
| [0013](docs/adr/0013-circuit-breaker-per-job-type.md) | Per-job-type circuit breaker with closed/open/half-open states |

New ADRs should follow [the template](0000-template.md) and be numbered
sequentially.
