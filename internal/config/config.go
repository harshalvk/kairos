// Package config centralizes env-var configuration shared
// across every kairos command, so connection strings and identifiers
// are read consistently instead of each cmd/ reimplementing the same
// os.Getenv-with-fallback pattern
package config

import "os"

// Config holds the env-derived settings every kairos process
// needed to connect to its dependencies
type Config struct {
	RedisAddr    string
	PostgresDSN  string
	TenantID     string
	NodeID       string
	OTELEndpoint string
}

// Load reads configuration from the env, applying the same
// local-development defaults every cmd/ has historically used
func Load() (Config, error) {
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			hostname = "unknown"
		}
		nodeID = hostname
	}

	tenantID := os.Getenv("TENANT_ID")
	if tenantID == "" {
		tenantID = "default"
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	pgDSN := os.Getenv("POSTGRES_DSN")
	if pgDSN == "" {
		// #nosec G101 -- local dev default only, not a ral
		// credential
		pgDSN = "postgres://kairos:kairos@localhost:5432/kairos"
	}

	otelEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if otelEndpoint == "" {
		otelEndpoint = "localhost:4318"
	}

	return Config{
		RedisAddr:    redisAddr,
		PostgresDSN:  pgDSN,
		TenantID:     tenantID,
		NodeID:       nodeID,
		OTELEndpoint: otelEndpoint,
	}, nil
}
