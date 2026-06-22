// Package metrics provides Prometheus instrumentation for the gateway.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// RequestsTotal counts all inference requests by status.
	RequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "inference_requests_total",
		Help: "Total number of inference requests, labeled by status.",
	}, []string{"status"})

	// RequestDuration tracks end-to-end latency histograms (P50/P95/P99).
	RequestDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "inference_request_duration_seconds",
		Help:    "End-to-end latency of inference requests in seconds.",
		Buckets: prometheus.DefBuckets,
	})

	// BatchSize tracks how many requests were grouped per batch dispatch.
	BatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "inference_batch_size",
		Help:    "Number of requests grouped into each batch.",
		Buckets: []float64{1, 2, 4, 8, 16, 32},
	})

	// ActiveWorkers tracks the number of healthy worker nodes.
	ActiveWorkers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "inference_active_worker_count",
		Help: "Number of currently healthy backend worker nodes.",
	})

	// QueueDepth tracks the current number of requests waiting in the batcher.
	QueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "inference_queue_depth",
		Help: "Current number of requests waiting in the dynamic batching queue.",
	})

	// WorkerConnections tracks active in-flight requests per worker.
	WorkerConnections = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "inference_worker_active_connections",
		Help: "Number of active in-flight requests on each worker node.",
	}, []string{"worker"})
)

// Handler returns the Prometheus HTTP metrics handler.
func Handler() http.Handler {
	return promhttp.Handler()
}
