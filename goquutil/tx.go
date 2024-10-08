/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package goquutil

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/acronis/go-appkit/httpserver/middleware"
	golibslog "github.com/acronis/go-appkit/log"
	"github.com/doug-martin/goqu/v9"
)

// PreQueryFuncT is type for pre query hook function
type PreQueryFuncT func(ctx context.Context, query string, args ...interface{}) string

// PostQueryFuncT is type for post query hook function
type PostQueryFuncT func(ctx context.Context, startedAt time.Time, err error, query string, args ...interface{})

// PreQueryHook will be executed before actual query execution
var PreQueryHook PreQueryFuncT

// PostQueryHook will be executed after actual query execution
var PostQueryHook PostQueryFuncT

// ContextProvider is an interface that defines a method for obtaining a context.Context.
// Implementing types should return the context.Context representing
// the execution context of the operation or task.
type ContextProvider interface {
	Context() context.Context
}

type cancellableTxQuerier struct {
	ctx context.Context
	tx  *goqu.TxDatabase
}

func newCancellableTxQuerier(ctx context.Context, tx *goqu.TxDatabase) Querier {
	return &cancellableTxQuerier{ctx: ctx, tx: tx}
}

func (q *cancellableTxQuerier) Exec(query string, args ...interface{}) (sql.Result, error) {
	if PreQueryHook != nil {
		query = PreQueryHook(q.ctx, query, args...)
	}

	start := time.Now().UTC()
	res, err := q.tx.ExecContext(q.ctx, query, args...)

	if PostQueryHook != nil {
		PostQueryHook(q.ctx, start, err, query, args...)
	}
	return res, err
}

func (q *cancellableTxQuerier) Query(query string, args ...interface{}) (*sql.Rows, error) {
	if PreQueryHook != nil {
		query = PreQueryHook(q.ctx, query, args...)
	}

	start := time.Now().UTC()
	res, err := q.tx.QueryContext(q.ctx, query, args...)

	if PostQueryHook != nil {
		PostQueryHook(q.ctx, start, err, query, args...)
	}
	return res, err
}

func (q *cancellableTxQuerier) QueryRow(query string, args ...interface{}) *sql.Row {
	if PreQueryHook != nil {
		query = PreQueryHook(q.ctx, query, args...)
	}

	start := time.Now().UTC()
	res := q.tx.QueryRowContext(q.ctx, query, args...)

	if PostQueryHook != nil {
		PostQueryHook(q.ctx, start, nil, query, args...)
	}
	return res
}

func (q *cancellableTxQuerier) Context() context.Context {
	return q.ctx
}

// DB is a wrapper for goqu.Database
type DB struct {
	db                          *goqu.Database
	ctx                         context.Context
	txOpts                      *sql.TxOptions
	logger                      golibslog.FieldLogger
	loggingCtx                  string
	loggingTimeThresholdBeginTx time.Duration
}

// NewDB returns tx wrapper for goqu.Database
func NewDB(ctx context.Context, db *goqu.Database) *DB {
	return &DB{db: db, ctx: ctx}
}

// DoInTx opens db tx and runs worker func within its context
func (d *DB) DoInTx(worker func(q Querier) error) error {
	start := time.Now()

	tx, err := d.db.BeginTx(d.ctx, d.txOpts)
	if err != nil {
		return err
	}

	if d.logger != nil {
		elapsed := time.Since(start).Milliseconds()
		var level = golibslog.LevelDebug
		if elapsed > d.loggingTimeThresholdBeginTx.Milliseconds() {
			level = golibslog.LevelInfo
		}
		d.logger.AtLevel(level, func(logFunc golibslog.LogFunc) {
			logFunc(
				fmt.Sprintf("opened DB transaction (%s) in %dms", d.loggingCtx, elapsed),
				golibslog.Int64("duration_ms", elapsed),
			)
		})
		if d.ctx != nil {
			loggingParams := middleware.GetLoggingParamsFromContext(d.ctx)
			if loggingParams != nil {
				loggingParams.AddTimeSlotInt("open_db_transaction_ms", elapsed)
			}
		}
	}

	err = tx.Wrap(func() error {
		q := newCancellableTxQuerier(d.ctx, tx)
		workerErr := worker(q)
		start = time.Now()
		return workerErr
	})
	if d.logger != nil {
		elapsed := time.Since(start).Milliseconds()
		d.logger.Debug(
			fmt.Sprintf("closed DB transaction (%s) in %dms", d.loggingCtx, elapsed),
			golibslog.Int64("duration_ms", elapsed),
		)
		if d.ctx != nil {
			loggingParams := middleware.GetLoggingParamsFromContext(d.ctx)
			if loggingParams != nil {
				loggingParams.AddTimeSlotInt("closed_db_transaction_ms", elapsed)
			}
		}
	}
	return err
}

// WithTxOpts allows passing additional options for opened tx
func (d *DB) WithTxOpts(txOpts *sql.TxOptions) *DB {
	d.txOpts = txOpts
	return d
}

// WithLogging enables logging of time consumed on openning/getting DB connection from pool
func (d *DB) WithLogging(logger golibslog.FieldLogger, loggingCtx string, loggingTimeThresholdBeginTx time.Duration) *DB {
	d.logger = logger
	d.loggingCtx = loggingCtx
	d.loggingTimeThresholdBeginTx = loggingTimeThresholdBeginTx
	return d
}
