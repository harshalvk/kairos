package job_test

import (
	"encoding/json"
	"testing"

	"github.com/harshalvk/kairos/internal/job"
)

func FuzzNew_NeverPanicsOnPayload(f *testing.F) {
	// seed corpus: a mix of valid and edge-cage json payloads to start
	// the fuzzzer's mutation from
	f.Add(`{"to":"test@example.com"}`)
	f.Add(`{}`)
	f.Add(`null`)
	f.Add(`[]`)
	f.Add(`""`)
	f.Add(`{"nested":{"deeply":{"a":1}}}`)

	f.Fuzz(func(t *testing.T, payload string) {
		// job.New should never panic regardless of payload content - evern
		// if it's not valid json, since payload is stored as raw bytes and
		// only interpreted later by a job-specific handler, not New itself
		j := job.New("fuzz_test", json.RawMessage(payload), 3)

		if j.ID == "" {
			t.Error("job.New produced an empty ID")
		}
		if j.Status != job.StatusPending {
			t.Errorf("expected StatusPending, got %v", j.Status)
		}
	})
}
