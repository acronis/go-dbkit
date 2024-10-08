/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package dbrutil

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/acronis/go-appkit/retry"
	"github.com/cenkalti/backoff/v4"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/acronis/go-dbkit"
)

// Open opens database (using dbr query builder) with specified configuration parameters
// and verifies (if ping argument is true) that connection can be established.
func Open(cfg *db.Config, ping bool, eventReceiver dbr.EventReceiver) (*dbr.Connection, error) {
	driver, dsn := cfg.DriverNameAndDSN()
	conn, err := dbr.Open(driver, dsn, eventReceiver)
	if err != nil {
		return nil, err
	}

	if err := db.InitOpenedDB(conn.DB, cfg, ping); err != nil {
		return nil, err
	}

	return conn, nil
}

// TxCommitError is a error that may occur when committing transaction is failed.
type TxCommitError struct {
	Inner error
}

// Unwrap unwraps internal error for IsRetryable algorithm
func (e *TxCommitError) Unwrap() error {
	return e.Inner
}

// Error returns a string representation of TxCommitError.
func (e *TxCommitError) Error() string {
	return fmt.Sprintf("error while committing transaction: %s", e.Inner)
}

// TxRollbackError is an error that may occur when rollback has failed.
type TxRollbackError struct {
	Inner error
}

// Unwrap unwraps internal error for IsRetryable algorithm
func (e *TxRollbackError) Unwrap() error {
	return e.Inner
}

// Error returns a string representation of TxRollbackError.
func (e *TxRollbackError) Error() string {
	return fmt.Sprintf("error while transaction rollback: %s", e.Inner)
}

// TxBeginError is a error that may occur when begging transaction is failed.
type TxBeginError struct {
	Inner error
}

// Unwrap unwraps internal error for IsRetryable algorithm
func (e *TxBeginError) Unwrap() error {
	return e.Inner
}

// Error returns a string representation of TxBeginError.
func (e *TxBeginError) Error() string {
	return fmt.Sprintf("error while begging transaction: %s", e.Inner)
}

// TxRunner can begin a new transaction and provides the ability to execute code inside already started one.
// Wrappers from dbr query builder are used.
type TxRunner interface {
	BeginTx(ctx context.Context) (*dbr.Tx, error)
	DoInTx(ctx context.Context, fn func(runner dbr.SessionRunner) error) error
}

// TxSession contains Session form dbr query builder (represents a business unit of execution (e.g. a web request or some worker's job))
// and options for starting transactions.
type TxSession struct {
	*dbr.Session
	TxOpts *sql.TxOptions
}

// NewTxSession creates a new TxSession.
func NewTxSession(conn *dbr.Connection, opts *sql.TxOptions) *TxSession {
	return &TxSession{
		Session: conn.NewSession(nil),
		TxOpts:  opts,
	}
}

// NewTxRunner creates a new object of TxRunner.
func NewTxRunner(conn *dbr.Connection, opts *sql.TxOptions, eventReceiver dbr.EventReceiver) TxRunner {
	return &TxSession{
		Session: conn.NewSession(eventReceiver),
		TxOpts:  opts,
	}
}

// BeginTx begins a new transaction.
func (s *TxSession) BeginTx(ctx context.Context) (*dbr.Tx, error) {
	return s.Session.BeginTx(ctx, s.TxOpts)
}

// DoInTx begins a new transaction, calls passed function and do commit or rollback
// depending on whether the function returns an error or not.
func (s *TxSession) DoInTx(ctx context.Context, fn func(runner dbr.SessionRunner) error) error {
	if s.Connection.Dialect == dialect.SQLite3 {
		// race of ctx cancel with transaction begin leads to 'cannot start a transaction within a transaction'
		// https://github.com/mattn/go-sqlite3/pull/765
		ctx = context.TODO()
	}
	tx, err := s.BeginTx(ctx)
	if err != nil {
		return &TxBeginError{err}
	}

	defer tx.RollbackUnlessCommitted()
	if err := fn(tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return &TxCommitError{err}
	}

	return nil
}

// NewRetryableTxSession creates a new RetryableTxSession.
func NewRetryableTxSession(conn *dbr.Connection, opts *sql.TxOptions, p retry.Policy) *RetryableTxSession {
	return &RetryableTxSession{
		TxSession: TxSession{
			Session: conn.NewSession(nil),
			TxOpts:  opts,
		},
		policy: p,
		log:    conn.EventReceiver,
	}
}

// NewRetryableTxRunner creates a new object of TxRunner with retries.
func NewRetryableTxRunner(conn *dbr.Connection, opts *sql.TxOptions, eventReceiver dbr.EventReceiver, p retry.Policy) TxRunner {
	return &RetryableTxSession{
		TxSession: TxSession{
			Session: conn.NewSession(eventReceiver),
			TxOpts:  opts,
		},
		policy: p,
		log:    eventReceiver,
	}
}

// RetryableTxSession is a wrapper around TxSession that makes transaction executed with DoInTx retryable.
type RetryableTxSession struct {
	TxSession
	policy retry.Policy
	log    dbr.EventReceiver
}

// DoInTx implements TxRunner.
func (s *RetryableTxSession) DoInTx(ctx context.Context, fn func(runner dbr.SessionRunner) error) error {
	var notify backoff.Notify
	if s.log != nil {
		notify = func(err error, d time.Duration) {
			_ = s.log.EventErrKv("backoff", err, map[string]string{"duration_ms": strconv.Itoa(int(d.Milliseconds()))})
		}
	}
	return retry.DoWithRetry(ctx, s.policy, db.GetIsRetryable(s.Driver()), notify, func(ctx context.Context) error {
		return s.TxSession.DoInTx(ctx, fn)
	})
}

// ParseAnnotationInQuery parses annotation from comments in SQL query with specified prefix.
// If SQL query contains multiple annotations, they will be concatenated with "|" character.
func ParseAnnotationInQuery(query, prefix string, modifier func(string) string) string {
	var left int
	var buf bytes.Buffer
	for left < len(query) {
		if !strings.HasPrefix(query[left:], "/*") {
			break
		}
		left += 2
		r := strings.Index(query[left:], "*/")
		if r == -1 {
			break
		}
		right := left + r
		annotation := strings.TrimSpace(query[left:right])
		if annotation != "" && strings.HasPrefix(annotation, prefix) {
			if modifier != nil {
				annotation = modifier(annotation)
			}
			if annotation != "" {
				if buf.Len() != 0 {
					buf.WriteString("|") // nolint: gosec
				}
				buf.WriteString(annotation) // nolint: gosec
			}
		}
		left = right + 2
		for left < len(query) && (query[left] == ' ' || query[left] == '\n') {
			left++
		}
	}
	return buf.String()
}
