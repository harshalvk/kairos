# ADR 0028: Consistent versioning across gRPC, HTTP, and Go package APIs

## Status
Accepted

## Context
Kairos has three public API surfaces (gRPC, admin HTTP, Go packages)
that had inconsistent versioning discipline: gRPC was versioned from
the start (kairos.v1, ADR 0021), but the HTTP API had no version
prefix, and Go package versioning was implicit in module tags with no
documented policy.

## Decision
Add a /v1 URL prefix to every admin HTTP API route (except /healthz,
which stays unversioned as an infra-consumed liveness check).
Document the existing gRPC versioning policy and Go module SemVer
policy explicitly in CONTRIBUTING.md, with a single stated rule: a
breaking change adds a new version alongside the old one for
proto/HTTP, and triggers a major Go version bump for pkg/.

## Consequences
- The HTTP API prefix change is itself a breaking change to every
  existing route — tagged as BREAKING CHANGE for semantic-release,
  correctly triggering a major version bump on this release.
- Every future HTTP/gRPC breaking change now has a clear, precedented
  pattern to follow (v2 alongside v1) rather than requiring a fresh
  design discussion each time.
