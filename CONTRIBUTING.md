# Contributing

## Prerequisites

- Go 1.22+
- Docker + Docker Compose
- [`golangci-lint`](https://golangci-lint.run/welcome/install/)
- [`lefthook`](https://github.com/evilmartians/lefthook)

## Setup

```bash
git clone https://github.com/harshalvk/kairos.git
cd kairos
lefthook install
make docker-up
make migrate
```

## Common commands

Run `make help` for the full list. The ones you'll use most:

| Command              | What it does                                  |
| -------------------- | --------------------------------------------- |
| `make run-worker`    | Start the worker pool                         |
| `make run-producer`  | Enqueue a test job                            |
| `make run-scheduler` | Start the delayed-job scheduler               |
| `make lint`          | Run golangci-lint                             |
| `make fmt`           | Format with goimports                         |
| `make test`          | Run tests (unit + testcontainers integration) |
| `make vuln`          | Check dependencies for known CVEs             |
| `make sec`           | Run gosec security scan                       |

## Before committing

`lefthook` runs `fmt`/`lint`/`vet` automatically on commit, and `test`/`vuln` on push. If a hook fails, fix the issue and re-stage — don't bypass with `--no-verify` unless you have a specific reason (and explain it in the commit message if you do).

## Commit messages

This repo follows [Conventional Commits](https://www.conventionalcommits.org/): `type(scope): description`. Common types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `build`, `ci`. Scope is usually the package touched (`worker`, `queue`, `store`) or `dev`/`db`/`build` for tooling.

## Architectural decisions

Significant design decisions are recorded in [`docs/adr/`](docs/adr/README.md) as ADRs (Context → Decision → Consequences). If you're proposing a change that reverses or significantly alters an existing decision, add a new ADR referencing the one it supersedes rather than just changing code silently.

## Package layout

- `internal/job` — core Job domain model, no dependencies on other packages
- `internal/queue` — Redis-backed queue (pending, dead-letter, delayed)
- `internal/store` — Postgres job history persistence
- `internal/metrics` — Prometheus metrics definitions
- `internal/worker` — worker pool, retry/backoff, dead-letter logic
- `cmd/*` — entrypoints; thin wiring only, no business logic

## Quick start (devcontainer)

If you use VS Code or GitHub Codespaces, you don't need to install Go, Redis, Postgres, or any of the tooling below manually — open this repo in a devcontainer (`Ctrl+Shift+P` → "Dev Containers: Reopen in Container" in VS Code, or "Create codespace" on GitHub) and everything is pre-configured: Go 1.22, golangci-lint, lefthook (pre-commit hooks installed automatically), govulncheck, gosec, and live Redis/Postgres instances.

## Manual setup

(existing prerequisites/setup content stays below this, for anyone not using the devcontainer)

## Keeping local and CI tooling in sync

If you update Go or golangci-lint locally, update the matching version
in `.github/workflows/ci.yml` in the same commit. Local and CI drifting
apart on tool versions is the most common source of "works locally,
fails in CI" — check `go version` and `golangci-lint --version` against
what's pinned in ci.yml before troubleshooting anything else.

## Logging conventions

- `internal/*` packages return errors; they don't log directly (the exception is `internal/worker`, which owns retry/dead-letter decisions worth recording).
- `cmd/*` entrypoints use structured `slog` (via `internal/logging`) for all operational logging — startup, shutdown, errors.
- Plain `fmt.Println`/`fmt.Printf` is reserved for direct CLI result output a human runs the command to see (e.g. `cmd/producer`'s "enqueued: <id>"), not for logging events.

## Releasing

Releases are fully automated — you never manually run `git tag`.

On every push to `master`, semantic-release inspects commits since the
last release and decides whether a new version is warranted:

- `feat:` commits → minor version bump
- `fix:`, `perf:`, `refactor:` commits → patch version bump
- `docs:`, `test:`, `chore:`, `ci:`, `build:` commits → no release
- A commit with a `BREAKING CHANGE:` footer → major version bump

If a release is warranted, semantic-release creates the tag and GitHub
Release with generated notes, then goreleaser builds and attaches
cross-platform binaries. Write correct Conventional Commit messages —
that's the only lever that controls what gets released and when.

## Opening issues and PRs

Bug reports and feature requests use the structured forms in the issue
template picker — please don't skip required fields, they exist
because past debugging in this project has consistently needed that
exact information (see the CI troubleshooting history for a real
example of why "what did you actually run" matters).

PRs follow the template in `.github/PULL_REQUEST_TEMPLATE.md` — What /
Why / Changes / How to verify / Known limitations, the same structure
every commit and ADR in this project already follows.
