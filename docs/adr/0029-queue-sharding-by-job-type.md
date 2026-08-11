# ADR 0029: Queue sharding by job type via consistent hashing

## Status
Accepted

## Context
A single Redis instance was Kairos's scaling ceiling — flagged
indirectly across several earlier ADRs (0006, 0011, 0013) but never
addressed. Large deployments with high job volume need to distribute
load across multiple Redis instances.

## Decision
Shard by job type (FNV-1a hash mod shard count), not job ID. This
keeps per-job-type in-memory state (rate limiter, circuit breaker,
both process-local) correctly co-located — every instance of a given
job type always lands on the same shard, so that state stays correct
without needing to become distributed itself.

## Consequences
- Dequeue latency regresses under sharding: BRPOP's near-instant
  blocking wakeup is replaced by short-interval (200ms) polling across
  each worker's relevant shards, since Redis BRPOP cannot span
  multiple separate client connections in one call. A worker serving
  job types spread across 3 shards has up to ~600ms worst-case latency
  to notice a new job. This is an explicit, accepted tradeoff of
  sharding — not a silent regression.
- Uses simple hash % N, not a consistent-hashing ring with virtual
  nodes — shard count is a deployment-time configuration, not expected
  to change dynamically, so the added complexity of a ring (which
  mainly helps minimize redistribution on shard count changes) isn't
  justified yet.
- A job type's load is bound to exactly one shard — a single very
  high-volume job type cannot itself be spread across multiple shards.
  If one job type's volume alone exceeds a single Redis instance's
  capacity, this scheme doesn't help; sharding by job ID (with the
  rate-limit/circuit-breaker distribution cost that implies) would be
  the fix, deferred as a much larger future change.
- Backward compatible: NewSharded([]*redis.Client{one}) behaves
  identically to the pre-sharding New(rdb) — existing single-Redis
  deployments are unaffected.
