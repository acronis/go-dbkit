/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package dbkit

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/acronis/go-appkit/config"
	"github.com/acronis/go-appkit/retry"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		ping    bool
		wantErr bool
	}{
		{
			name: "successful open with ping",
			cfg: &Config{
				Dialect:         DialectSQLite,
				SQLite:          SQLiteConfig{Path: ":memory:"},
				MaxOpenConns:    10,
				MaxIdleConns:    5,
				ConnMaxLifetime: config.TimeDuration(time.Minute * 10),
			},
			ping:    true,
			wantErr: false,
		},
		{
			name: "error on open",
			cfg: &Config{
				Dialect:         Dialect("unknown"),
				SQLite:          SQLiteConfig{Path: ":memory:"},
				MaxOpenConns:    10,
				MaxIdleConns:    5,
				ConnMaxLifetime: config.TimeDuration(time.Minute * 10),
			},
			ping:    false,
			wantErr: true,
		},
		{
			name: "error on ping",
			cfg: &Config{
				Dialect:         DialectSQLite,
				SQLite:          SQLiteConfig{Path: "internal"}, // directory is not a valid path
				MaxOpenConns:    10,
				MaxIdleConns:    5,
				ConnMaxLifetime: config.TimeDuration(time.Minute * 10),
			},
			ping:    true,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbConn, err := Open(tt.cfg, tt.ping)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, dbConn)
			}
		})
	}
}

func TestDoInTx(t *testing.T) {
	tests := []struct {
		name         string
		initMock     func(m sqlmock.Sqlmock)
		fn           func(tx *sql.Tx) error
		wantErr      error
		wantPanicErr error
	}{
		{
			name: "success",
			initMock: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectCommit()
			},
			fn: func(tx *sql.Tx) error {
				return nil
			},
		},
		{
			name: "error on begin",
			initMock: func(m sqlmock.Sqlmock) {
				m.ExpectBegin().WillReturnError(fmt.Errorf("begin error"))
			},
			fn: func(tx *sql.Tx) error {
				return nil
			},
			wantErr: fmt.Errorf("begin tx: begin error"),
		},
		{
			name: "error on commit",
			initMock: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectCommit().WillReturnError(fmt.Errorf("commit error"))
			},
			fn: func(tx *sql.Tx) error {
				return nil
			},
			wantErr: fmt.Errorf("commit tx: commit error"),
		},
		{
			name: "error in func",
			initMock: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectRollback()
			},
			fn: func(tx *sql.Tx) error {
				return fmt.Errorf("fn error")
			},
			wantErr: fmt.Errorf("fn error"),
		},
		{
			name: "panic in func",
			initMock: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectRollback()
			},
			fn: func(tx *sql.Tx) error {
				panic(fmt.Errorf("panic"))
			},
			wantPanicErr: fmt.Errorf("panic"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer func() {
				require.NoError(t, mock.ExpectationsWereMet())
			}()

			tt.initMock(mock)

			if tt.wantPanicErr != nil {
				require.PanicsWithError(t, tt.wantPanicErr.Error(), func() {
					_ = DoInTx(context.Background(), db, tt.fn)
				})
				return
			}
			err = DoInTx(context.Background(), db, tt.fn)
			if tt.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, tt.wantErr.Error())
		})
	}
}

func TestDoInTxWithRetryPolicy(t *testing.T) {
	retryableError := errors.New("retryable error")

	retryPolicy := retry.NewConstantBackoffPolicy(time.Millisecond*50, 3)

	tests := []struct {
		name       string
		initMock   func(m sqlmock.Sqlmock)
		fnProvider func() func(tx *sql.Tx) error
		wantErr    error
	}{
		{
			name: "success, no retry attempts",
			initMock: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
				m.ExpectCommit()
			},
			fnProvider: func() func(tx *sql.Tx) error {
				return func(tx *sql.Tx) error {
					_, queryErr := tx.Query("SELECT 1")
					return queryErr
				}
			},
		},
		{
			name: "success after retry",
			initMock: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectRollback()
				m.ExpectBegin()
				m.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
				m.ExpectCommit()
			},
			fnProvider: func() func(tx *sql.Tx) error {
				var attempts int
				return func(tx *sql.Tx) error {
					attempts++
					if attempts < 2 {
						return retryableError
					}
					_, queryErr := tx.Query("SELECT 1")
					return queryErr
				}
			},
		},
		{
			name: "fail, no retry on non-retryable error",
			initMock: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectRollback()
			},
			fnProvider: func() func(tx *sql.Tx) error {
				return func(tx *sql.Tx) error {
					return fmt.Errorf("non-retryable error")
				}
			},
			wantErr: fmt.Errorf("non-retryable error"),
		},
		{
			name: "fail, max retry attempts exceeded",
			initMock: func(m sqlmock.Sqlmock) {
				// 4 attempts: 1 initial + 3 retries
				m.ExpectBegin()
				m.ExpectRollback()
				m.ExpectBegin()
				m.ExpectRollback()
				m.ExpectBegin()
				m.ExpectRollback()
				m.ExpectBegin()
				m.ExpectRollback()
			},
			fnProvider: func() func(tx *sql.Tx) error {
				return func(tx *sql.Tx) error {
					return retryableError
				}
			},
			wantErr: retryableError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)

			UnregisterAllIsRetryableFuncs(db.Driver())
			RegisterIsRetryableFunc(db.Driver(), func(err error) bool {
				return errors.Is(err, retryableError)
			})

			tt.initMock(mock)

			err = DoInTx(context.Background(), db, tt.fnProvider(), WithRetryPolicy(retryPolicy))
			if tt.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tt.wantErr.Error())
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
