# ADR 0015: OpenTelemetry tracing via OTLP HTTP, global tracer provider

## Status
Accepted

## Context
Structured logging (ADR 0014) captures individual events well, but
doesn't show the full timing/causal relationship of a job's journey
through enqueue, dequeue, rate limiting, circuit breaker checks, the
handler itself, and outcome recording — especially across process
boundaries in a multi-node deployment.

## Decision
Add OpenTelemetry tracing via otlptracehttp, exporting to a local
Jaeger instance (via its built-in OTLP receiver) for development.
Worker pool wraps each job's processing in a job.process span, with a
nested job.handler child span isolating handler execution time from
Kairos's own overhead (rate limiting, circuit breaker, persistence).
Uses OTel's global TracerProvider pattern rather than threading a
tracer through every function signature, matching OTel's own idiomatic
API design.

## Consequences
- Nested spans make it possible to distinguish "job was slow because
  of the handler" from "job was slow because of Kairos overhead" at a
  glance in a trace viewer.
- Tracing setup failure (unreachable collector) logs a warning and
  continues rather than failing startup — observability infrastructure
  being down should not stop job processing.
- Global TracerProvider means any code anywhere in the process can
  create spans via otel.Tracer(...) without explicit wiring, at the
  cost of being a genuine global — an intentional, OTel-idiomatic
  exception to generally avoiding globals elsewhere in this codebase.
- Only internal/worker currently creates spans; queue/store operations
  are not yet individually traced as child spans (e.g. the Redis
  BRPOP, the Postgres RecordStatus call) — a natural next increment if
  finer-grained timing within a single job's processing becomes
  needed.
