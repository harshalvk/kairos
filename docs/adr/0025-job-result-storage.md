# ADR 0025: Optional job result storage via Job.SetResult

## Status
Accepted

## Context
A job could previously only signal success or failure — no way to
return a value (a computed total, a generated resource's URL) back to
whatever enqueued it.

## Decision
Add an optional Result field on Job, set via j.SetResult(v) inside a
handler (mutation, not a changed Handler return signature — avoids a
breaking change across every existing handler). Persisted to
job_history.result (JSONB) on successful completion. Retrievable via
Store.GetResult, pkg/kairos's Result method, and GET /jobs/{id}/result.

## Consequences
- Requires Postgres (WithPostgresDSN) — a Kairos deployment running
  Redis-only has no result storage, since Result never round-trips
  through Redis after the job completes and is removed from the
  pending queue.
- No push notification on completion — callers must poll Result (or a
  future WaitForResult helper); Redis pub/sub-based notification is a
  natural follow-up, not implemented here.
- SetResult uses mutation rather than changing Handler's return
  signature specifically to avoid a breaking change across every
  existing handler in examples/, cmd/worker, and pkg/kairos.
