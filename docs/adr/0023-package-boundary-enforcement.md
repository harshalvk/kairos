# ADR 0023: Enforce package dependency boundaries via go-arch-lint

## Status
Accepted

## Context
The internal/ restructuring established an intended dependency
direction (job and metrics as leaves; queue/store depending only on
job; worker as an orchestration layer depending broadly; pkg/
kairosclient never reaching into internal/) purely by convention and
ADR documentation. Nothing mechanically prevented a future change from
violating it — Go's compiler only catches actual import cycles, not
architecturally-wrong-but-acyclic dependencies.

## Decision
Add go-arch-lint with an explicit component/dependency declaration
matching the intended layering, run in CI, pre-commit (via lefthook),
and available via `make arch-lint`.

## Consequences
- Architectural drift (e.g. a data-layer package like queue or store
  accidentally depending on the orchestration layer) is now caught
  mechanically, not just in code review.
- The most safety-critical rule this config enforces: pkg/kairosclient
  may only depend on pkg/kairospb, never on any internal/ package —
  directly protecting the public API boundary ADR 0021 established.
- The config requires manual maintenance as new internal/ packages are
  added — a new package with no entry in .go-arch-lint.yml would need
  its allowed dependencies declared explicitly, which is a deliberate
  friction point: it forces a conscious decision about a new package's
  place in the layering rather than it silently inheriting broad
  access.
