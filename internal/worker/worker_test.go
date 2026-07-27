package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBackoffDuration(t *testing.T) {
	tests := []struct {
		name    string
		attempt int
		want    time.Duration
	}{
		{"first attempt", 1, 1 * time.Second},
		{"second attempt", 2, 2 * time.Second},
		{"third attempt", 3, 4 * time.Second},
		{"fourth attempt", 4, 8 * time.Second},
		{"caps at max", 10, 30 * time.Second},
		{"caps at max, way beyond", 20, 30 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := backoffDuration(tt.attempt)
			assert.Equal(t, tt.want, got)
		})
	}
}

func BenchmarkBackoffDuration(b *testing.B) {
	for b.Loop() {
		backoffDuration(5)
	}
}
