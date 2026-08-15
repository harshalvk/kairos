# ADR 0033: Benchmark results tracked over time via Pushgateway + Grafana

## Status
Accepted

## Decision
Parse `go test -bench -benchmem` output (regex-based, not -json) and
push ns/op, B/op, allocs/op as Prometheus gauges via Pushgateway —
the standard pattern for exposing short-lived batch job results to a
pull-based Prometheus, since the process exits before any scrape
could happen against it directly. Run nightly in CI, visualized in a
new Grafana dashboard.

## Consequences
- Regex parsing is simpler but more fragile than -json parsing — a Go
  toolchain change to benchmark output format could silently break
  it. Acceptable for a first version; -json is the natural hardening
  step if this becomes more heavily relied upon.
- CI runner hardware is shared and variable — ns/op trends from GitHub
  Actions runners are a rough indicator (catches a benchmark 10x
  slower than last week), not a precise regression-detection tool
  (won't reliably catch a 5% regression). A dedicated, consistent
  benchmark runner would be needed for that level of rigor.
