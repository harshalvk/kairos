# ADR 0024: Multi-tenancy via context-propagated tenant namespace

## Status

Accepted

## Context

A single Kairos deployment previously served exactly one logical
application — all Redis keys and Postgres rows were global, with no
isolation mechanism if multiple tenants needed to share a deployment.

## Decision

Add internal/tenant carrying tenant identity via context.Context,
following the same propagation pattern already established for
logging and tracing. Every Redis key gets namespaced by tenant
(kairos:<tenant>:pending:high, etc.); Postgres tables gain a tenant_id
column, defaulted to 'default' for backward compatibility, and every
query scopes by it explicitly. A worker pool serves exactly one tenant
per instance, configured at startup via TENANT_ID.

## Consequences

- Existing single-tenant deployments continue working unchanged —
  tenant.DefaultTenant matches the SQL column default exactly, so no
  data migration or behavior change occurs for anyone not setting
  TENANT_ID.
- Tenant IDs are validated to exclude colons specifically, since
  Kairos's own Redis keys are colon-delimited — an unvalidated tenant
  ID could otherwise construct a key colliding with another tenant's
  or the system's own key structure.
- One worker pool instance serves exactly one tenant — genuine
  multi-tenant routing within a single pool (one pool dequeuing across
  many tenants' queues) is a materially bigger change deferred as a
  future increment, not implemented here.
- gRPC service, admin HTTP API, and CLI tools (cmd/deadletter,
  cmd/producer) do not yet accept/propagate a tenant ID — currently
  only cmd/worker is tenant-aware end to end. Extending tenant
  awareness to the other interfaces is a known follow-up.
- ~~gRPC service, admin HTTP API, and CLI tools do not yet accept/propagate a tenant ID~~
  UPDATE: admin API reads X-Tenant-ID header via middleware; gRPC
  reads a tenant-id metadata key via a unary interceptor;
  pkg/kairosclient.WithTenant sets it on outgoing calls. CLI tools
  (cmd/producer, cmd/deadletter, cmd/seed-recurring) still default to
  tenant.DefaultTenant with no way to target another tenant — a
  --tenant flag would close this remaining gap if needed.
