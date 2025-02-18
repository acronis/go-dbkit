/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package dbkit

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/acronis/go-appkit/retry"
)

// Open opens a new database connection using the provided configuration.
// If ping is true, it will check the connection by sending a ping to the database.
func Open(cfg *Config, ping bool) (*sql.DB, error) {
	driver, dsn := cfg.DriverNameAndDSN()
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	return db, InitOpenedDB(db, cfg, ping)
}

// InitOpenedDB initializes early opened *sql.DB instance.
func InitOpenedDB(db *sql.DB, cfg *Config, ping bool) error {
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(time.Duration(cfg.ConnMaxLifetime))
	if ping {
		if err := db.Ping(); err != nil {
			return err
		}
	}
	return nil
}

type doInTxOptions struct {
	txOpts      *sql.TxOptions
	retryPolicy retry.Policy
}

// DoInTxOption is a functional option for DoInTx.
type DoInTxOption func(*doInTxOptions)

// WithTxOptions sets transaction options for DoInTx.
func WithTxOptions(txOpts *sql.TxOptions) DoInTxOption {
	return func(opts *doInTxOptions) {
		opts.txOpts = txOpts
	}
}

// WithRetryPolicy sets retry policy for DoInTx.
func WithRetryPolicy(policy retry.Policy) DoInTxOption {
	return func(opts *doInTxOptions) {
		opts.retryPolicy = policy
	}
}

// DoInTx begins a new transaction, calls passed function and do commit or rollback
// depending on whether the function returns an error or not.
func DoInTx(ctx context.Context, dbConn *sql.DB, fn func(tx *sql.Tx) error, options ...DoInTxOption) (err error) {
	var opts doInTxOptions
	for _, opt := range options {
		opt(&opts)
	}
	if opts.retryPolicy == nil {
		return doInTx(ctx, dbConn, fn, opts.txOpts)
	}
	return retry.DoWithRetry(ctx, opts.retryPolicy, GetIsRetryable(dbConn.Driver()), nil, func(ctx context.Context) error {
		return doInTx(ctx, dbConn, fn, opts.txOpts)
	})
}

func doInTx(ctx context.Context, dbConn *sql.DB, fn func(tx *sql.Tx) error, txOpts *sql.TxOptions) (err error) {
	var tx *sql.Tx
	if tx, err = dbConn.BeginTx(ctx, txOpts); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
		if err != nil {
			_ = tx.Rollback()
			return
		}
		if err = tx.Commit(); err != nil {
			err = fmt.Errorf("commit tx: %w", err)
		}
	}()
	return fn(tx)
}
