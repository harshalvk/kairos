// Package metrics defines the Prometheus metrics exposed by the job queue.
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics exposed by the job queue, registered globally on package init.
var (
	JobsProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "karios_jobs_processed_total",
			Help: "Total number of jobs processed, labeled by type and outcome.",
		},
		[]string{"type", "outcome"},
	)

	// JobDuration measures how long each job's handler takes to run.
	JobDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "karios_job_duration_seconds",
			Help:    "Duration of job handler exectuion in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"type"},
	)

	// QueueDepth reports how many jobs are currently waiting in the
	// pending queue.
	QueueDepth = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kairos_pending_depth",
			Help: "Current number of jobs waiting in the pending queue.",
		},
	)

	// CircuitState reports the current circuit breaker state per job
	// type: 0 = closed, 1 = open, 2 = half-open.
	CircuitState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kairos_circuit_breaker_state",
			Help: "Current circuit breaker state per job type (0=closed, 1=open, 2=half-open).",
		},
		[]string{"type"},
	)

	// internal/metrics/metrics.go
	PoisonPillsDetected = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kairos_poison_pills_detected_total",
			Help: "Total number of jobs dead-lettered due to repeated unexplained worker disappearance.",
		},
		[]string{"type"},
	)

	OverflowDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kairos_overflow_depth",
			Help: "Current number of jobs sitting in the backpressure overflow queue.",
		},
		[]string{"tenant_id"},
	)
)

func init() {
	prometheus.MustRegister(JobsProcessed, JobDuration, QueueDepth, CircuitState, PoisonPillsDetected, OverflowDepth)
}
