# Examples

Runnable examples demonstrating Kairos's capabilities. Each is self-contained — `cd` into a directory and `go run .` (requires Redis running locally; `make docker-up` from the repo root).

| Example | Demonstrates |
|---|---|
| [basic](basic/) | Enqueue, dequeue, worker pool, retries with backoff |
| [priority-queue](priority-queue/) | High/default/low priority dequeue ordering |
| [job-dependencies](job-dependencies/) | DAG-style job chaining (B runs only after A succeeds) |
| [resilience](resilience/) | Idempotency keys, rate limiting, and circuit breaker together |
