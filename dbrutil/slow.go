/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package dbrutil

import (
	"time"

	"github.com/acronis/go-appkit/log"
	"github.com/gocraft/dbr/v2"
)

// SlowQueryLogEventReceiverOpts consists options for SlowQueryLogEventReceiver.
type SlowQueryLogEventReceiverOpts struct {
	AnnotationPrefix   string
	AnnotationModifier func(string) string
}

// SlowQueryLogEventReceiver implements the dbr.EventReceiver interface and logs long SQL queries.
// To be logged SQL query should be annotated (comment starting with specified prefix).
type SlowQueryLogEventReceiver struct {
	*dbr.NullEventReceiver
	logger             log.FieldLogger
	longQueryTime      time.Duration
	annotationPrefix   string
	annotationModifier func(string) string
}

// NewSlowQueryLogEventReceiverWithOpts creates a new SlowQueryLogEventReceiver with additinal options.
func NewSlowQueryLogEventReceiverWithOpts(logger log.FieldLogger, longQueryTime time.Duration,
	options SlowQueryLogEventReceiverOpts) *SlowQueryLogEventReceiver {
	return &SlowQueryLogEventReceiver{
		NullEventReceiver:  &dbr.NullEventReceiver{},
		logger:             logger,
		longQueryTime:      longQueryTime,
		annotationPrefix:   options.AnnotationPrefix,
		annotationModifier: options.AnnotationModifier,
	}
}

// NewSlowQueryLogEventReceiver creates a new SlowQueryLogEventReceiver.
func NewSlowQueryLogEventReceiver(logger log.FieldLogger, longQueryTime time.Duration,
	annotationPrefix string) *SlowQueryLogEventReceiver {
	opts := SlowQueryLogEventReceiverOpts{
		AnnotationPrefix: annotationPrefix,
	}
	return NewSlowQueryLogEventReceiverWithOpts(logger, longQueryTime, opts)
}

// TimingKv is called when SQL query is executed. It receives the duration of how long the query takes,
// parses annotation from SQL comment and logs last if execution time is long.
func (er *SlowQueryLogEventReceiver) TimingKv(eventName string, nanoseconds int64, kvs map[string]string) {
	if nanoseconds < er.longQueryTime.Nanoseconds() {
		return
	}
	annotation := ParseAnnotationInQuery(kvs["sql"], er.annotationPrefix, er.annotationModifier)
	if annotation == "" {
		return
	}
	er.logger.Warn("slow SQL query",
		log.String("annotation", annotation),
		log.Int64("duration_ms", nanoseconds/int64(time.Millisecond)),
	)
}
