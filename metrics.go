/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package dbkit

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// PrometheusMetricsLabelQuery is a label name for SQL query in Prometheus metrics.
const PrometheusMetricsLabelQuery = "query"

// DefaultQueryDurationBuckets is default buckets into which observations of executing SQL queries are counted.
var DefaultQueryDurationBuckets = []float64{0.001, 0.01, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// MetricsCollectorOpts represents an options for PrometheusMetrics.
type MetricsCollectorOpts struct {
	// Namespace is a namespace for metrics. It will be prepended to all metric names.
	Namespace string

	// QueryDurationBuckets is a list of buckets into which observations of executing SQL queries are counted.
	QueryDurationBuckets []float64

	// ConstLabels is a set of labels that will be applied to all metrics.
	ConstLabels prometheus.Labels

	// CurryingLabelNames is a list of label names that will be curried with the provided labels.
	// See PrometheusMetrics.MustCurryWith method for more details.
	// Keep in mind that if this list is not empty,
	// PrometheusMetrics.MustCurryWith method must be called further with the same labels.
	// Otherwise, the collector will panic.
	CurriedLabelNames []string
}

// PrometheusMetrics represents collector of metrics.
type PrometheusMetrics struct {
	QueryDurations *prometheus.HistogramVec
}

// NewPrometheusMetrics creates a new metrics collector.
func NewPrometheusMetrics() *PrometheusMetrics {
	return NewPrometheusMetricsWithOpts(MetricsCollectorOpts{})
}

// NewPrometheusMetricsWithOpts is a more configurable version of creating PrometheusMetrics.
func NewPrometheusMetricsWithOpts(opts MetricsCollectorOpts) *PrometheusMetrics {
	queryDurationBuckets := opts.QueryDurationBuckets
	if queryDurationBuckets == nil {
		queryDurationBuckets = DefaultQueryDurationBuckets
	}
	labelNames := append(make([]string, 0, len(opts.CurriedLabelNames)+1), opts.CurriedLabelNames...)
	labelNames = append(labelNames, PrometheusMetricsLabelQuery)
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
	return &PrometheusMetrics{QueryDurations: queryDurations}
}

// MustCurryWith curries the metrics collector with the provided labels.
func (pm *PrometheusMetrics) MustCurryWith(labels prometheus.Labels) *PrometheusMetrics {
	return &PrometheusMetrics{
		QueryDurations: pm.QueryDurations.MustCurryWith(labels).(*prometheus.HistogramVec),
	}
}

// MustRegister does registration of metrics collector in Prometheus and panics if any error occurs.
func (pm *PrometheusMetrics) MustRegister() {
	prometheus.MustRegister(pm.QueryDurations)
}

// Unregister cancels registration of metrics collector in Prometheus.
func (pm *PrometheusMetrics) Unregister() {
	prometheus.Unregister(pm.QueryDurations)
}

// AllMetrics returns a list of metrics of this collector. This can be used to register these metrics in push gateway.
func (pm *PrometheusMetrics) AllMetrics() []prometheus.Collector {
	return []prometheus.Collector{pm.QueryDurations}
}

// ObserveQueryDuration observes the duration of executing SQL query.
func (pm *PrometheusMetrics) ObserveQueryDuration(query string, duration time.Duration) {
	pm.QueryDurations.With(prometheus.Labels{PrometheusMetricsLabelQuery: query}).Observe(duration.Seconds())
}
