package chaos

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/harshalvk/kairos/internal/circuitbreaker"
)

// TestChaos_CircuitBreakerStopsHammeringDownDependency verifies the
// claim in ADR 0013: once the circuit opens, most subsequent attempts
// never actually reach a failing dependency — they're rejected by
// Allow before any call is made.
func TestChaos_CircuitBreakerStopsHammeringDownDependency(t *testing.T) {
	var callCount atomic.Int32
	failingServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer failingServer.Close()

	cb := circuitbreaker.New(3, 1*time.Second)
	callDependency := func() error {
		resp, err := http.Get(failingServer.URL) //nolint:noctx // chaos test, simplicity over strictness
		if err != nil {
			return err
		}
		defer func() {
			if closeErr := resp.Body.Close(); closeErr != nil {
				t.Logf("failed to close response body: %v", closeErr)
			}
		}()
		if resp.StatusCode >= 300 {
			return fmt.Errorf("status %d", resp.StatusCode)
		}
		return nil
	}

	// Hammer it 20 times as if 20 job attempts happened in quick
	// succession — the claim being tested: the circuit should open well
	// before all 20 actually reach the dependency.
	for i := 0; i < 20; i++ {
		if cb.Allow("flaky_dependency") {
			if err := callDependency(); err != nil {
				cb.RecordFailure("flaky_dependency")
			}
		}
	}

	assert.Less(t, callCount.Load(), int32(10), "circuit breaker should have prevented most of the 20 attempts from reaching the dependency")
	assert.Equal(t, circuitbreaker.StateOpen, cb.StateOf("flaky_dependency"))
}
