package benchreport_test

import (
	"testing"

	"github.com/harshalvk/kairos/internal/benchreport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse_ExtractsBenchmarkResults(t *testing.T) {
	output := `
goos: linux
goarch: amd64
BenchmarkBackoffDuration-8      50000000    23.4 ns/op    0 B/op    0 allocs/op
BenchmarkJobMarshal-8            2000000   612.0 ns/op  128 B/op    3 allocs/op
PASS
ok      github.com/harshalvk/kairos/internal/worker    2.145s
`
	results, err := benchreport.Parse(output)
	require.NoError(t, err)
	require.Len(t, results, 2)

	assert.Equal(t, "BenchmarkBackoffDuration", results[0].Name)
	assert.InDelta(t, 23.4, results[0].NsPerOp, 0.01)

	assert.Equal(t, "BenchmarkJobMarshal", results[1].Name)
	assert.Equal(t, int64(128), results[1].BytesPerOp)
	assert.Equal(t, int64(3), results[1].AllocsPerOp)
}

func TestParse_IgnoresNonBenchmarkLines(t *testing.T) {
	results, err := benchreport.Parse("goos: linux\nPASS\nok  pkg  1.2s\n")
	require.NoError(t, err)
	assert.Empty(t, results)
}
