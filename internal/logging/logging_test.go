package logging_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/harshalvk/kairos/internal/logging"
)

func TestFromContext_ReturnsAttachedLogger(t *testing.T) {
	logger := logging.New("test-node")
	ctx := logging.WithContext(context.Background(), logger)

	got := logging.FromContext(ctx)
	assert.Same(t, logger, got)
}

func TestFromContext_ReturnsDefaultWhenNoneAttached(t *testing.T) {
	got := logging.FromContext(context.Background())
	assert.Equal(t, slog.Default(), got)
}
