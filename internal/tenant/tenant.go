// Package tenant provides tenant identity propagation via context, so
// multi-tenant isolation doesn't require threading a tenantID parameter
// through every function signature across the codebase
package tenant

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
)

type contextKey struct{}

var idKey = contextKey{}

// DefaultTenant is used when no tenant is explicitly set - preserves
// backward compatibility for single-tanant deployments and existing
// tests that don't set up tenant context
const DefaultTenant = "default"

// Registry tracks known tenant IDs, backed by redis, so processes that
// neeed to operate across all tenants (like the scheduler) know which
// namespaces exist without requiring static configuration
type Registry struct {
	rdb *redis.Client
}

const registryKey = "kairos:tenants"

// NewRegistry creates a tenant registry backed by rdb
func NewRegistry(rdb *redis.Client) *Registry {
	return &Registry{rdb: rdb}
}

// Register records tenantID as known/active. called whenever a job is
// enqueued for a tenant, so the registry stays current automatcially
func (r *Registry) Register(ctx context.Context, tenantID string) error {
	return r.rdb.SAdd(ctx, registryKey, tenantID).Err()
}

// List returns all known tenant IDs
func (r *Registry) List(ctx context.Context) ([]string, error) {
	return r.rdb.SMembers(ctx, registryKey).Result()
}

// WithContext attaches tenantID to ctx
func WithContext(ctx context.Context, teanantID string) context.Context {
	return context.WithValue(ctx, idKey, teanantID)
}

// FromContext returns the tenant ID attached to ctx, or DefaultTenant if
// none was set
func FromContext(ctx context.Context) string {
	if id, ok := ctx.Value(idKey).(string); ok && id != "" {
		return id
	}
	return DefaultTenant
}

// ErrInvalidTenant is returned when a tenant ID fails validation
var ErrInvalidTenant = errors.New("tenant id must be non-empty and contain no colon characters")

// Validate checks that a tenant ID is safe to use as redis key
// segment - specifically, that id can't bbe used to inject extra key
// segments (e.g. a tenant ID of "a:pending" could otherwise collide
// with kairos's own key structure)
func Validate(tenantID string) error {
	if tenantID == "" {
		return ErrInvalidTenant
	}
	for _, r := range tenantID {
		if r == ':' {
			return ErrInvalidTenant
		}
	}
	return nil
}
