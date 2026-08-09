# ADR 0026: Webhook notifications via a decoupled delivery queue

## Status

Accepted

## Decision

Job lifecycle events (completed/failed/dead_letter) can trigger an
HTTP callback, configured per job via WebhookConfig. Delivery is
decoupled from job processing: worker.process only enqueues a
delivery (cheap, non-blocking); a separate Dispatcher.Run loop
performs the actual HTTP call with its own bounded (5-attempt) retry.

## Consequences

- A slow or hanging webhook endpoint cannot slow down or block actual
  job throughput — the coupling point is a Redis LPUSH, not a
  synchronous HTTP call inside the worker's hot path.
- Webhook retry (bounded, sleep-based re-push) is deliberately simpler
  and lower-stakes than job retry (durable, sorted-set-backed) — the
  job's own outcome is already determined by the time a webhook fires;
  the notification failing doesn't change that outcome, just delays
  visibility of it.
- No delivery guarantee beyond 5 attempts — a webhook endpoint down for
  longer than that window silently misses the notification, with only
  a log line as the record. A dead-letter-style webhook delivery queue
  (mirroring job dead-lettering) would close this gap if needed.
