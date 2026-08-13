# ADR 0031: Backpressure via configurable max queue depth

## Status
Accepted

## Context
Nothing previously stopped the pending queue from growing unboundedly
if producers outpaced workers — a real risk surfaced concretely by
Cycle 1's k6 load testing, where sustained 50 req/s enqueue against 5
default workers visibly grew queue depth with no ceiling.

## Decision
Add optional per-priority max-depth enforcement at Enqueue time, with
a choice of two policies: PolicyReject (fail the enqueue with
ErrQueueFull) or PolicyOverflow (route to a separate, unsharded
overflow list). cmd/scheduler periodically drains overflow back into
the pending queue, re-checking backpressure on every item so the drain
self-throttles rather than immediately overflowing again.

## Consequences
- Unlimited by default — existing deployments see no behavior change
  unless WithBackpressure is explicitly configured.
- PolicyReject pushes the problem back to the caller immediately
  (simpler, but requires the caller to handle ErrQueueFull sensibly —
  retry later, drop, alert). PolicyOverflow absorbs the burst but
  delays the problem rather than solving it — if producers
  sustainably outpace workers rather than just bursting, overflow
  itself grows unboundedly, just in a different Redis key. Overflow
  is a shock absorber for bursts, not a capacity solution for
  sustained overload.
- Overflow depth is now a monitored metric specifically so a
  perpetually growing overflow (the sustained-overload case above) is
  visible, not a silent, forgotten safety valve.
