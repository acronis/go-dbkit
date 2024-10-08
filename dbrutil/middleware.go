/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package dbrutil

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	"github.com/acronis/go-appkit/httpserver/middleware"
	"github.com/gocraft/dbr/v2"
)

type ctxKey int

const ctxKeyTxRunner ctxKey = iota

// NewTxRunnerFunc - factory function for create TxRunner objects.
type NewTxRunnerFunc func(conn *dbr.Connection, opts *sql.TxOptions, eventReceiver dbr.EventReceiver) TxRunner

// TxRunnerMiddlewareOpts represents an options for the TxRunnerMiddleware middleware.
type TxRunnerMiddlewareOpts struct {
	ContextKey   interface{}
	SlowQueryLog struct {
		MinTime          time.Duration
		AnnotationPrefix string
	}
	NewTxRunner NewTxRunnerFunc
}

type txRunnerHandler struct {
	next   http.Handler
	dbConn *dbr.Connection
	txOpts *sql.TxOptions
	opts   TxRunnerMiddlewareOpts
}

// TxRunnerMiddleware is a middleware that injects TxRunner to the request's context.
func TxRunnerMiddleware(dbConn *dbr.Connection, isolationLevel sql.IsolationLevel) func(next http.Handler) http.Handler {
	return TxRunnerMiddlewareWithOpts(dbConn, isolationLevel, TxRunnerMiddlewareOpts{})
}

// TxReadOnlyRunnerMiddleware is a middleware that injects TxRunner to the request's context (readonly).
func TxReadOnlyRunnerMiddleware(dbConn *dbr.Connection, isolationLevel sql.IsolationLevel) func(next http.Handler) http.Handler {
	return TxReadOnlyRunnerMiddlewareWithOpts(dbConn, isolationLevel, TxRunnerMiddlewareOpts{})
}

// applyDefaults - applies default values for nil options.
func applyDefaults(opts *TxRunnerMiddlewareOpts) {
	if opts.ContextKey == nil {
		opts.ContextKey = ctxKeyTxRunner
	}
	if opts.NewTxRunner == nil {
		opts.NewTxRunner = NewTxRunner
	}
}

// TxRunnerMiddlewareWithOpts is a more configurable version of the TxRunnerMiddleware middleware.
func TxRunnerMiddlewareWithOpts(
	dbConn *dbr.Connection, isolationLevel sql.IsolationLevel, opts TxRunnerMiddlewareOpts,
) func(next http.Handler) http.Handler {
	applyDefaults(&opts)
	return func(next http.Handler) http.Handler {
		return &txRunnerHandler{next, dbConn, &sql.TxOptions{Isolation: isolationLevel}, opts}
	}
}

// TxReadOnlyRunnerMiddlewareWithOpts is a more configurable version of the TxReadOnlyRunnerMiddleware middleware.
func TxReadOnlyRunnerMiddlewareWithOpts(dbConn *dbr.Connection, isolationLevel sql.IsolationLevel,
	opts TxRunnerMiddlewareOpts,
) func(next http.Handler) http.Handler {
	applyDefaults(&opts)
	return func(next http.Handler) http.Handler {
		return &txRunnerHandler{next, dbConn, &sql.TxOptions{Isolation: isolationLevel, ReadOnly: true}, opts}
	}
}

func (m *txRunnerHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	reqCtx := r.Context()

	dbEventReceiver := m.dbConn.EventReceiver
	if m.opts.SlowQueryLog.MinTime > 0 {
		slowLogEventReceiver := NewSlowQueryLogEventReceiver(
			middleware.GetLoggerFromContext(reqCtx), m.opts.SlowQueryLog.MinTime, m.opts.SlowQueryLog.AnnotationPrefix)
		if dbEventReceiver != nil {
			dbEventReceiver = NewCompositeReceiver([]dbr.EventReceiver{dbEventReceiver, slowLogEventReceiver})
		} else {
			dbEventReceiver = slowLogEventReceiver
		}
	}

	dbSess := m.opts.NewTxRunner(m.dbConn, m.txOpts, dbEventReceiver)
	m.next.ServeHTTP(rw, r.WithContext(NewContextWithTxRunnerByKey(reqCtx, dbSess, m.opts.ContextKey)))
}

// NewContextWithTxRunner creates a new context with TxRunner.
func NewContextWithTxRunner(parentCtx context.Context, txRunner TxRunner) context.Context {
	return NewContextWithTxRunnerByKey(parentCtx, txRunner, ctxKeyTxRunner)
}

// NewContextWithTxRunnerByKey creates a new context and put TxRunner there by specified key.
func NewContextWithTxRunnerByKey(parentCtx context.Context, txRunner TxRunner, ctxKey interface{}) context.Context {
	return context.WithValue(parentCtx, ctxKey, txRunner)
}

// GetTxRunnerFromContext extracts TxRunner from the context.
func GetTxRunnerFromContext(ctx context.Context) TxRunner {
	return GetTxRunnerFromContextByKey(ctx, ctxKeyTxRunner)
}

// GetTxRunnerFromContextByKey extracts TxRunner from the context by specified key.
func GetTxRunnerFromContextByKey(ctx context.Context, ctxKey interface{}) TxRunner {
	return ctx.Value(ctxKey).(TxRunner)
}
