// Command benchpush runs `go test -bench` against the given packages,
// parses the results, and pushes them to a Prometheus Pushgateway.
package main

import (
	"context"
	"flag"
	"log"
	"os/exec"

	"github.com/harshalvk/kairos/internal/benchreport"
)

func main() {
	gatewayURL := flag.String("gateway", "http://localhost:9091", "Pushgateway URL")
	pkgs := flag.String("pkgs", "./...", "package pattern to benchmark")
	flag.Parse()

	ctx := context.Background()
	// #nosec G204 -- pkgs is an operator-supplied flag for a local dev
	// tool, not untrusted external input.
	cmd := exec.CommandContext(
		ctx,
		"go",
		"test",
		"-bench=.",
		"-benchmem",
		"-run=^$",
		*pkgs,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("go test -bench exited with error (may still have usable output): %v", err)
	}

	results, err := benchreport.Parse(string(output))
	if err != nil {
		log.Fatalf("failed to parse benchmark output: %v", err)
	}
	if len(results) == 0 {
		log.Fatal("no benchmark results found in output")
	}

	if err := benchreport.Push(*gatewayURL, "kairos_benchmarks", results); err != nil {
		log.Fatalf("failed to push results: %v", err)
	}
	log.Printf("pushed %d benchmark results to %s", len(results), *gatewayURL)
}
