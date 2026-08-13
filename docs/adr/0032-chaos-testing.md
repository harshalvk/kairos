# ADR 0032: Chaos testing to verify failure-recovery claims

## Status
Accepted

## Context
Several ADRs make explicit claims about failure recovery (0003:
delayed jobs survive worker restarts; 0019: leader election fails
over automatically; 0013: circuit breaker stops hammering a failing
dependency) — but none of these claims had ever been verified against
actually-failing infrastructure, only unit-tested against
happy-path/mocked scenarios.

## Decision
Add chaos/ — a top-level package (outside internal/, pkg/, since these
are system-level tests spanning multiple components) using
testcontainers' container restart capability to kill real
infrastructure mid-test and assert recovery actually happens. Run on
a schedule (daily) and via manual dispatch, not on every push — kept
separate from the fast normal test suite.
