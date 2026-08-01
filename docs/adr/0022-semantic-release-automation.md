# ADR 0022: Automated versioning via semantic-release, driven by Conventional Commits

## Status
Accepted

## Context
Version tags were previously created manually (`git tag v0.1.0`),
requiring a human to decide the correct next version and remember to
push it — error-prone and easy to forget, especially as release
cadence increases.

## Decision
Add semantic-release, driven by the conventionalcommits preset,
running on every push to master. It determines whether a release is
warranted and what version bump applies purely from commit message
types (feat/fix/perf/refactor trigger a release; docs/test/chore/ci/
build do not). goreleaser then builds cross-platform binaries only
when semantic-release actually created a new tag.

## Consequences
- Correct commit messages are now load-bearing, not just good
  practice — an incorrectly typed commit (e.g. "feat: fix a typo"
  instead of "fix: ...") directly causes an incorrect version bump.
- No more manual tagging step; releases happen automatically and more
  frequently, which is generally good for consumers (smaller, more
  frequent releases) but means every push to master that contains a
  feat/fix is now release-triggering — worth being deliberate about
  what lands on master directly versus in a feature branch.
- refactor/perf map to patch, not minor, since they don't change the
  public contract — only feat (new capability) bumps minor. This is an
  interpretation choice, documented here so it's not a mystery later
  why a refactor-heavy release only bumped patch.
