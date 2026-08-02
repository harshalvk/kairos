// Package tenant provides tenant identity propagation via context, so
// multi-tenant isolation doesn't require threading a tenantID parameter
// through every function signature across the codebase
package tenant

import (
	"context"
	"errors"
)

type contextKey struct{}

var idKey = contextKey{}

// DefaultTenant is used when no tenant is explicitly set - preserves
// backward compatibility for single-tanant deployments and existing
// tests that don't set up tenant context
const DefaultTenant = "default"

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
