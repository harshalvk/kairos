// Package benchreport parses `go test -bench -benchmem` output and
// pushes ns/op and B/op metrics to a Prometheus Pushgateway, so
// benchmark results become a trackable time series in Grafana instead
// of one-off numbers read from a terminal
package benchreport

import (
	"bufio"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

// Result is one parsed benchmark line
type Result struct {
	Name        string
	NsPerOp     float64
	BytesPerOp  int64
	AllocsPerOp int64
}

// benchLineRE matches a standard result
var benchLineRE = regexp.MustCompile(`^(Benchmark\S+)\s+\d+\s+([\d.]+)\s+ns/op(?:\s+([\d.]+)\s+B/op\s+(\d+)\s+allocs/op)?`)

// Parse reads raw `go test -bench -benchmem` output and extracts every
// benchmark result line
func Parse(output string) ([]Result, error) {
	var results []Result
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		line := scanner.Text()
		m := benchLineRE.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		ns, err := strconv.ParseFloat(m[2], 64)
		if err != nil {
			return nil, fmt.Errorf("parse ns/op in %q: %w", line, err)
		}
		r := Result{Name: strings.SplitN(m[1], "-", 2)[0], NsPerOp: ns}
		if m[3] != "" {
			bytesPerOp, err := strconv.ParseFloat(m[3], 64)
			if err == nil {
				r.BytesPerOp = int64(bytesPerOp)
			}
		}
		if m[4] != "" {
			allocs, err := strconv.ParseInt(m[4], 10, 64)
			if err == nil {
				r.AllocsPerOp = allocs
			}
		}
		results = append(results, r)
	}
	return results, scanner.Err()
}

// Push sends results to a Prometheus Pushgateway at gatewarURL, tagged
// with jobName (typically "kairos_benchmarks") so they show up as a
// distinct grouping
func Push(gatewayURL, jobName string, results []Result) error {
	nsGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "kairos_bench_ns_per_op", Help: "Benchmark nanoseconds per operation"}, []string{"benchmark"},
	)
	bytesGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "kairos_bench_bytes_per_op", Help: "Benchmark bytes allocated per operation."},
		[]string{"benchmark"},
	)
	allocsGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "kairos_bench_allocs_per_op", Help: "Benchmark allocations per operation."},
		[]string{"benchmark"},
	)

	for _, r := range results {
		nsGauge.WithLabelValues(r.Name).Set(r.NsPerOp)
		bytesGauge.WithLabelValues(r.Name).Set(float64(r.BytesPerOp))
		allocsGauge.WithLabelValues(r.Name).Set(float64(r.AllocsPerOp))
	}

	return push.New(gatewayURL, jobName).
		Collector(nsGauge).
		Collector(bytesGauge).
		Collector(allocsGauge).
		Push()
}
