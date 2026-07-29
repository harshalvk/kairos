# ADR 0017: Load testing via k6 against the admin API

## Status
Accepted

## Context
No systematic way existed to answer "how many jobs/sec can this
system absorb" or "what's the latency distribution under sustained
load" — only informal manual testing.

## Decision
Add a POST /jobs endpoint to the admin API (previously enqueue-only
via the CLI producer), and k6 scripts targeting it and the
dead-letter read path, using constant-arrival-rate scenarios with
explicit p95 latency and failure-rate thresholds.

## Consequences
- Gives objective, repeatable numbers instead of informal impressions
  of "feels fast enough."
- constant-arrival-rate isolates throughput from concurrency, letting
  a specific req/s target be tested directly rather than inferred from
  VU count and response time.
- The admin API's POST /jobs endpoint now duplicates what
  cmd/producer already does via direct queue.Enqueue calls — added
  specifically to make HTTP-based load testing possible, not because
  the API needed an enqueue capability for its own sake. Worth noting
  since it's the API surface a load test needs, not necessarily
  something an admin dashboard would otherwise expose.
- Thresholds are not yet wired into CI (would need a running
  worker+Redis+Postgres stack, which the existing CI test job doesn't
  fully stand up for this purpose) — currently a manual, local-only
  check.
