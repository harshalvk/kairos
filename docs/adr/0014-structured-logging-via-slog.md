# ADR 0014: Structured JSON logging via slog, propagated through context

## Status
Accepted

## Context
log.Printf-based logging produced human-readable but unstructured
output — no consistent fields (job_id, job_type, worker_id), nothing
machine-parseable for log aggregation or filtering by a specific job's
lifecycle.

## Decision
Use the standard library's log/slog with a JSON handler. A logger is
constructed once per process (tagged with node_id) and attached to
context.Context; process() enriches it further with job_id/job_type/
attempt fields, so every log line for a given job's handling carries
consistent structured context without threading extra parameters
through every function signature.

## Consequences
- Log lines are machine-parseable (JSON) by default — usable directly
  with log aggregation tooling, at the cost of being less immediately
  readable in a raw terminal (mitigated locally via `| jq`).
- slog chosen over zap/zerolog specifically to avoid a new dependency
  and match where the Go ecosystem has standardized since 1.21;
  performance is more than sufficient at this project's scale.
- Context-based logger propagation means call sites don't need to pass
  job_id/job_type explicitly to every log call, but does mean logger
  enrichment is implicit — a function receiving ctx needs to know to
  call logging.FromContext(ctx) rather than log.Printf directly, which
  is a discipline change from the previous pattern.
