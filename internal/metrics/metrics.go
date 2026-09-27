// Package metrics defines the Prometheus metrics exposed by the service.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	Submitted prometheus.Counter
	Completed prometheus.Counter
	Failed    *prometheus.CounterVec // label status: failed | dead_letter
	Retried   prometheus.Counter
	Recovered prometheus.Counter
	Duration  *prometheus.HistogramVec // label outcome
}

// New registers the metrics on reg. Tests pass a fresh registry to stay isolated.
func New(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		Submitted: f.NewCounter(prometheus.CounterOpts{
			Name: "jobs_submitted_total",
			Help: "Jobs accepted by the API (idempotent replays are not counted).",
		}),
		Completed: f.NewCounter(prometheus.CounterOpts{
			Name: "jobs_completed_total",
			Help: "Jobs that completed successfully.",
		}),
		Failed: f.NewCounterVec(prometheus.CounterOpts{
			Name: "jobs_failed_total",
			Help: "Jobs that reached a failure state, by final status.",
		}, []string{"status"}),
		Retried: f.NewCounter(prometheus.CounterOpts{
			Name: "jobs_retried_total",
			Help: "Automatic retries scheduled after a retryable error.",
		}),
		Recovered: f.NewCounter(prometheus.CounterOpts{
			Name: "jobs_recovered_total",
			Help: "Jobs recovered after their worker's lease expired.",
		}),
		Duration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "job_processing_duration_seconds",
			Help:    "Handler execution time, by outcome.",
			Buckets: prometheus.ExponentialBuckets(0.005, 4, 10), // 5ms .. ~22min
		}, []string{"outcome"}),
	}
}
