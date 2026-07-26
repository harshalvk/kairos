// Package logging provides a shared structured logger configuration used
// accross all kairos commands
package logging

import (
	"context"
	"log/slog"
	"os"
)

// contextKey is an unexporeted type to avoid collisions with other
// packages context keys
type contextKey struct{}

var loggerKey = contextKey{}

// New creates a structured json logger, tagged with the given node ID so
// log lines from multiple worker processes can be disthinguished
func New(nodeID string) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	return slog.New(handler).With(slog.String("node_id", nodeID))
}

// WithContext attaches a logger to ctx, retriveable later via FromContext
func WithContext(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, logger)
}

// FromContext returns the logger attached to ctx, or slog.Default() if
// none was attached
func FromContext(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value(loggerKey).(*slog.Logger); ok {
		return logger
	}
	return slog.Default()
}
