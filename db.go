/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package db

import (
	"context"
	"database/sql"
	"fmt"
)

// InitOpenedDB initializes early opened *sql.DB instance.
func InitOpenedDB(db *sql.DB, cfg *Config, ping bool) error {
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	if ping {
		if err := db.Ping(); err != nil {
			return err
		}
	}

	return nil
}

// DoInTx begins a new transaction, calls passed function and do commit or rollback
// depending on whether the function returns an error or not.
func DoInTx(ctx context.Context, dbConn *sql.DB, fn func(tx *sql.Tx) error) (err error) {
	return DoInTxWithOpts(ctx, dbConn, nil, fn)
}

// DoInTxWithOpts is a bit more configurable version of DoInTx that allows passing tx options.
func DoInTxWithOpts(ctx context.Context, dbConn *sql.DB, txOpts *sql.TxOptions, fn func(tx *sql.Tx) error) (err error) {
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
