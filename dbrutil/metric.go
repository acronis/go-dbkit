/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package dbrutil

import (
	"time"

	"github.com/gocraft/dbr/v2"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/acronis/go-dbkit"
)

// QueryMetricsEventReceiverOpts consists options for QueryMetricsEventReceiver.
type QueryMetricsEventReceiverOpts struct {
	AnnotationPrefix   string
	AnnotationModifier func(string) string
}

// QueryMetricsEventReceiver implements the dbr.EventReceiver interface and collects metrics about SQL queries.
// To be collected SQL query should be annotated (comment starting with specified prefix).
type QueryMetricsEventReceiver struct {
	*dbr.NullEventReceiver
	metricsCollector   *dbkit.MetricsCollector
	annotationPrefix   string
	annotationModifier func(string) string
}

// NewQueryMetricsEventReceiverWithOpts creates a new QueryMetricsEventReceiver with additinal options.
func NewQueryMetricsEventReceiverWithOpts(mc *dbkit.MetricsCollector, options QueryMetricsEventReceiverOpts) *QueryMetricsEventReceiver {
	return &QueryMetricsEventReceiver{
		metricsCollector:   mc,
		annotationPrefix:   options.AnnotationPrefix,
		annotationModifier: options.AnnotationModifier,
	}
}

// NewQueryMetricsEventReceiver creates a new QueryMetricsEventReceiver.
func NewQueryMetricsEventReceiver(mc *dbkit.MetricsCollector, annotationPrefix string) *QueryMetricsEventReceiver {
	options := QueryMetricsEventReceiverOpts{
		AnnotationPrefix: annotationPrefix,
	}
	return NewQueryMetricsEventReceiverWithOpts(mc, options)
}

// TimingKv is called when SQL query is executed. It receives the duration of how long the query takes,
// parses annotation from SQL comment and collects metrics.
func (er *QueryMetricsEventReceiver) TimingKv(eventName string, nanoseconds int64, kvs map[string]string) {
	annotation := ParseAnnotationInQuery(kvs["sql"], er.annotationPrefix, er.annotationModifier)
	if annotation == "" {
		return
	}
	labels := prometheus.Labels{dbkit.MetricsLabelQuery: annotation}
	er.metricsCollector.QueryDurations.With(labels).Observe(time.Duration(nanoseconds).Seconds())
}
