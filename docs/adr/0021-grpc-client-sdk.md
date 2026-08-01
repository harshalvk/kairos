# ADR 0021: gRPC service and client SDK under pkg/

## Status
Accepted

## Context
External services could previously only interact with Kairos via the
admin HTTP API (REST/JSON) or by shelling out to CLI tools — no typed,
language-agnostic RPC interface existed, and internal/ is structurally
unimportable outside this module by design.

## Decision
Define a versioned proto service (kairos.v1) covering enqueue, queue
depth, and dead-letter operations. Server implementation lives in
internal/grpcserver (only cmd/worker needs it); a hand-written client
wrapper lives in pkg/kairosclient, the first genuinely public Go API
this project exposes.

## Consequences
- pkg/kairosclient is now a real compatibility commitment — changes to
  its exported API affect any external consumer, unlike internal/
  packages which the compiler protects from outside use.
- Versioning the proto package (kairos.v1) from the start means a
  future breaking change becomes v2 alongside v1, not an in-place
  break — deliberately paying this cost now rather than retrofitting
  versioning after real external clients exist.
- The gRPC service and admin HTTP API (ADR 0016) both wrap the same
  queue.Queue operations independently, similar to the earlier
  cmd/deadletter + HTTP API duplication noted in that ADR — three
  interfaces (CLI, HTTP, gRPC) now exist over the same core operations,
  worth watching for drift if any of their individual validation logic
  needs to diverge.
- No authentication on the gRPC server either, matching the HTTP API's
  current state — same known gap, same reasoning (internal/trusted
  network only for now).
