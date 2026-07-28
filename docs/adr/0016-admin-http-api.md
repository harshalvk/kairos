# ADR 0016: Admin HTTP API wrapping existing queue operations

## Status
Accepted

## Context
Inspecting or managing the queue (listing/requeuing/purging
dead-lettered jobs, checking queue depth) previously required either
running cmd/deadletter or querying Redis/Postgres directly — no
programmatic interface existed for external tooling or a future
dashboard.

## Decision
Add internal/api exposing REST-ish HTTP endpoints over the same
queue.Queue operations cmd/deadletter already uses, served alongside
the existing /metrics endpoint on the worker process. Uses Go 1.22's
built-in http.ServeMux method+path routing rather than a third-party
router library.

## Consequences
- External tooling (or a future admin dashboard) can inspect/manage
  the queue without shelling out to a CLI or touching Redis/Postgres
  directly.
- /healthz gives container orchestrators something to poll, relevant
  the moment this runs anywhere beyond local dev.
- No authentication/authorization is implemented — this API is
  currently safe only on a trusted internal network. Adding auth
  (e.g. a bearer token, or restricting to the metrics port's existing
  network exposure pattern) is a known gap before this could be
  exposed beyond localhost/an internal network.
- The API and cmd/deadletter now both wrap the same queue.Queue
  methods independently — not a problem today, but worth noting if
  the CLI and API logic ever need to diverge (e.g. different
  validation), they'd need to be kept in sync deliberately.
