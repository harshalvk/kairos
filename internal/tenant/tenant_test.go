package tenant_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/harshalvk/kairos/internal/tenant"
)

func TestFromContext_ReturnsDefaultWhenNoneSet(t *testing.T) {
	assert.Equal(t, tenant.DefaultTenant, tenant.FromContext(context.Background()))
}

func TestFromContext_ReturnsAttachedTenant(t *testing.T) {
	ctx := tenant.WithContext(context.Background(), "acme-corp")
	assert.Equal(t, "acme-corp", tenant.FromContext(ctx))
}

func TestValidate_RejectsColon(t *testing.T) {
	assert.ErrorIs(t, tenant.Validate("acme:corp"), tenant.ErrInvalidTenant)
}

func TestValidate_RejectsEmpty(t *testing.T) {
	assert.ErrorIs(t, tenant.Validate(""), tenant.ErrInvalidTenant)
}

func TestValidate_AcceptsNormalID(t *testing.T) {
	assert.NoError(t, tenant.Validate("acme-corp"))
}
