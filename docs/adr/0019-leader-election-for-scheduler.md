# ADR 0019: Redis-based leader election for the scheduler

## Status
Accepted

## Context
ADR 0006 noted that running multiple cmd/scheduler instances was safe
but wasteful — all instances would poll PromoteDueJobs on the same
ticker, racing on the same Redis operations with only one doing useful
work per tick, and no automatic failover existed if the single
intended scheduler instance died.

## Decision
Add a leaderelection package using a Redis SETNX-based lock with TTL,
renewed periodically by whichever node holds it. Renew and Release use
Lua scripts to make "verify I still own this lock, then act" atomic,
preventing a node that has lost leadership from extending or deleting
a lock another node has since legitimately acquired.

## Consequences
- Exactly one scheduler instance actively promotes delayed jobs at a
  time; standby instances safely no-op until they acquire leadership.
- TTL expiry (15s default) provides automatic failover if the leader
  crashes without a graceful Release — bounded by the TTL, not
  instant, which is an accepted tradeoff for lock simplicity.
- The renewal interval (5s) is deliberately well below the TTL (15s)
  so a single missed renewal doesn't cause an unnecessary handoff.
- This is a single-Redis-instance lock, not a Redlock-style
  multi-instance quorum lock — sufficient here since Kairos already
  depends on a single Redis instance for everything else; a Redis
  outage already stops the whole system regardless of leader election.
