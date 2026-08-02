## What

<!-- One or two sentences: what does this PR change? -->

## Why

<!-- What problem does this solve, or what capability does it add? -->

## Changes

<!-- Bullet list of the actual changes, file/package by file/package -->

## How to verify

<!-- Exact commands a reviewer can run to confirm this works -->

```bash

```

## Checklist

- [ ] `make lint` passes
- [ ] `make test` passes (or `go test -race ./...` if you haven't touched anything requiring Docker)
- [ ] `go-arch-lint check` passes if you added/moved a package
- [ ] Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/) — this directly drives semantic versioning (see [ADR 0022](docs/adr/0022-semantic-release-automation.md))
- [ ] Added an ADR under `docs/adr/` if this changes queue semantics, storage schema, or a public API surface (`pkg/`)
- [ ] Updated relevant docs (`README.md`, `CONTRIBUTING.md`, or `docs/adr/`) if behavior changed

## Known limitations / follow-ups

<!-- Anything intentionally deferred, matching the pattern used throughout this project's ADRs -->
