# ADR 0020: Redis Streams as an alternative queue implementation (comparison, not replacement)

## Status
Accepted

## Context
ADR 0001 chose a Redis list (LPUSH/BRPOP) for the primary queue,
explicitly noting the tradeoff: no delivery tracking, so a job
delivered to a worker that crashes mid-processing (after BRPOP already
removed it) is silently lost. This ADR revisits that decision by
actually implementing the Streams-based alternative and evaluating it.

## Decision
Build internal/streamqueue as a parallel, independent implementation
using Redis Streams with a consumer group — XReadGroup for delivery,
XAck to confirm completion, XAutoClaim to reclaim messages left
pending by a crashed consumer. NOT wired into the primary worker pool;
kept as a standalone comparison (examples/streams-vs-lists) rather
than replacing internal/queue.

## Consequences
- Confirms Streams does close the exact gap ADR 0001 flagged: a
  message that's delivered but never acknowledged is visible
  (XPending) and reclaimable (XAutoClaim) by another consumer, unlike
  a list where BRPOP's removal is unconditional and final.
- Cost of that guarantee: every job requires an explicit Ack, adding a
  Redis round-trip per job that the list implementation doesn't need.
  Reclaim logic (a periodic XAutoClaim sweep) would also need to run
  somewhere — an additional moving part, similar in spirit to the
  scheduler process for delayed jobs.
- Deliberately NOT migrating internal/queue to Streams as part of this
  ADR: doing so would touch priority queues (ADR 0008), dependencies
  (ADR 0009), idempotency (ADR 0010), and every existing test — a
  large, risky change for a gap that, in practice, is partially
  mitigated already by retries (a lost job still gets naturally
  retried if the producer or a health check notices it never
  completed) even though it's not as clean as Streams' explicit
  tracking.
- Kept as a documented, working alternative so a future decision to
  migrate has a real implementation to build from, not just a
  hypothetical.
