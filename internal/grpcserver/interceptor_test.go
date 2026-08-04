package grpcserver_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"

	"github.com/harshalvk/kairos/internal/grpcserver"
	"github.com/harshalvk/kairos/internal/tenant"
)

func TestTenantInterceptor_ExtractsTenantFromMetadata(t *testing.T) {
	md := metadata.Pairs("tenant-id", "acme-corp")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	var gotTenant string
	handler := func(ctx context.Context, _ any) (any, error) {
		gotTenant = tenant.FromContext(ctx)
		return nil, nil
	}

	_, err := grpcserver.TenantInterceptor(ctx, nil, nil, handler)
	assert.NoError(t, err)
	assert.Equal(t, "acme-corp", gotTenant)
}

func TestTenantInterceptor_DefaultsWhenNoMetadata(t *testing.T) {
	var gotTenant string
	handler := func(ctx context.Context, _ any) (any, error) {
		gotTenant = tenant.FromContext(ctx)
		return nil, nil
	}

	_, err := grpcserver.TenantInterceptor(context.Background(), nil, nil, handler)
	assert.NoError(t, err)
	assert.Equal(t, tenant.DefaultTenant, gotTenant)
}
