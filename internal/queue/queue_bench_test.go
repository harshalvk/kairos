package queue_test

import (
	"encoding/json"
	"log"
	"testing"

	"github.com/harshalvk/kairos/internal/job"
)

func BenchmarkJobMarshal(b *testing.B) {
	payload, err := json.Marshal(map[string]string{"to": "test@example.com"})
	if err != nil {
		log.Fatalf("failed to marshal payload: %v", err)
	}
	j := job.New("send_email", payload, 3)

	for b.Loop() {
		if _, err := json.Marshal(j); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJobUnmarshal(b *testing.B) {
	payload, err := json.Marshal(map[string]string{"to": "test@example.com"})
	if err != nil {
		log.Fatalf("failed to marshal payload: %v", err)
	}
	j := job.New("send_email", payload, 3)
	data, err := json.Marshal(j)
	if err != nil {
		log.Fatalf("failed to marshal data: %v", err)
	}

	for b.Loop() {
		var got job.Job
		if err := json.Unmarshal(data, &got); err != nil {
			b.Fatal(err)
		}
	}
}
