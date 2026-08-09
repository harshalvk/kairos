# ADR 0027: Batch enqueue via a single Redis pipeline

## Status
Accepted

## Decision
Add Queue.EnqueueBatch, using a non-transactional Redis Pipeline (not
TxPipeline) to submit many independent jobs in one round-trip.
Dependencies and idempotency keys are explicitly unsupported per-item
in a batch — those need the atomicity/branching logic Enqueue,
EnqueueWithDependencies, and EnqueueIdempotent each provide
individually.

## Consequences
- Significant round-trip reduction for bulk enqueue (e.g. CSV import,
  bulk notification) — one network round-trip instead of N.
- Plain Pipeline chosen over TxPipeline deliberately: batch items are
  independent, with no "all or nothing" invariant to enforce, so the
  lighter-weight primitive is the right one — using TxPipeline would
  add transactional overhead for a guarantee this feature doesn't need.
- A caller needing per-item dependencies or idempotency must fall back
  to individual Enqueue calls in a loop — batch enqueue intentionally
  covers only the common, simple bulk case.
