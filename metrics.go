/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package db

import "github.com/prometheus/client_golang/prometheus"

// Prometheus labels.
const (
	MetricsLabelQuery = "query"
)

// DefaultQueryDurationBuckets is default buckets into which observations of executing SQL queries are counted.
var DefaultQueryDurationBuckets = []float64{0.001, 0.01, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// MetricsCollectorOpts represents an options for MetricsCollector.
type MetricsCollectorOpts struct {
	// Namespace is a namespace for metrics. It will be prepended to all metric names.
	Namespace string

	// QueryDurationBuckets is a list of buckets into which observations of executing SQL queries are counted.
	QueryDurationBuckets []float64

	// ConstLabels is a set of labels that will be applied to all metrics.
	ConstLabels prometheus.Labels

	// CurryingLabelNames is a list of label names that will be curried with the provided labels.
	// See MetricsCollector.MustCurryWith method for more details.
	// Keep in mind that if this list is not empty,
	// MetricsCollector.MustCurryWith method must be called further with the same labels.
	// Otherwise, the collector will panic.
	CurriedLabelNames []string
}

// MetricsCollector represents collector of metrics.
type MetricsCollector struct {
	QueryDurations *prometheus.HistogramVec
}

// NewMetricsCollector creates a new metrics collector.
func NewMetricsCollector() *MetricsCollector {
	return NewMetricsCollectorWithOpts(MetricsCollectorOpts{})
}

// NewMetricsCollectorWithOpts is a more configurable version of creating MetricsCollector.
func NewMetricsCollectorWithOpts(opts MetricsCollectorOpts) *MetricsCollector {
	queryDurationBuckets := opts.QueryDurationBuckets
	if queryDurationBuckets == nil {
		queryDurationBuckets = DefaultQueryDurationBuckets
	}
	labelNames := append(make([]string, 0, len(opts.CurriedLabelNames)+1), opts.CurriedLabelNames...)
	labelNames = append(labelNames, MetricsLabelQuery)
	queryDurations := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   opts.Namespace,
			Name:        "db_query_duration_seconds",
			Help:        "A histogram of the SQL query durations.",
			Buckets:     queryDurationBuckets,
			ConstLabels: opts.ConstLabels,
		},
		labelNames,
	)

	return &MetricsCollector{
		QueryDurations: queryDurations,
	}
}

// MustCurryWith curries the metrics collector with the provided labels.
func (c *MetricsCollector) MustCurryWith(labels prometheus.Labels) *MetricsCollector {
	return &MetricsCollector{
		QueryDurations: c.QueryDurations.MustCurryWith(labels).(*prometheus.HistogramVec),
	}
}

// MustRegister does registration of metrics collector in Prometheus and panics if any error occurs.
func (c *MetricsCollector) MustRegister() {
	prometheus.MustRegister(c.QueryDurations)
}

// Unregister cancels registration of metrics collector in Prometheus.
func (c *MetricsCollector) Unregister() {
	prometheus.Unregister(c.QueryDurations)
}

// AllMetrics returns a list of metrics of this collector. This can be used to register these metrics in push gateway.
func (c *MetricsCollector) AllMetrics() []prometheus.Collector {
	return []prometheus.Collector{
		c.QueryDurations,
	}
}
