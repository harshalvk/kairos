# ADR 0030: Poison-pill detection via claim-count tracking

## Status
Accepted

## Context
A job whose handler crashes the worker process itself (rather than
returning a clean error) was invisible to Kairos's existing retry
logic — the job is gone from Redis (per the known list-queue
limitation, ADR 0001), and if somehow retried, could go on to crash
worker after worker with no correlation between "workers keep dying"
and "this specific job is the cause."

## Decision
Track a claim count per job ID (Redis INCR + TTL) at the start of
processing, cleared on every clean outcome (success, normal failure,
dead-letter). If a job is claimed 3+ times without ever clearing —
meaning prior attempts disappeared without reporting an outcome — it's
conservatively dead-lettered rather than risking another crash.

## Consequences
- Cannot distinguish "this job caused the crash" from "the worker
  crashed for an unrelated reason at an unlucky moment" — a false
  positive dead-letters a fine job. Accepted: the alternative (an
  actual poison pill silently taking down an unbounded sequence of
  workers) is worse.
- Claim tracking is unsharded, deliberately living on shard 0 — it's
  small metadata (a counter per in-flight job ID), not queue data, so
  sharding it would add complexity without a real throughput need.
- The threshold (3) and claim TTL (10 minutes) are currently hardcoded
  — worth making configurable if different job types have very
  different normal processing durations.
