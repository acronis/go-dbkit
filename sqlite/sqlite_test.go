/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	"github.com/acronis/go-appkit/config"
	"github.com/acronis/go-appkit/retry"
	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"

	"github.com/acronis/go-dbkit"
)

const createFooTable = `create table foo (id integer not null primary key, name text)`

func TestSqliteRetryOnBusyError(t *testing.T) {
	const busyTimeoutMs = 100

	dbPath := t.TempDir() + "/TestSqliteRetryOnBusyError.db"
	dbDSN := fmt.Sprintf("%s?_busy_timeout=%d", dbPath, busyTimeoutMs)

	dbConn, err := sql.Open("sqlite3", dbDSN)
	require.NoError(t, err)
	defer func() { require.NoError(t, dbConn.Close()) }()

	_, err = dbConn.Exec(createFooTable)
	require.NoError(t, err)

	dbLocked := make(chan error)
	go execAndSleepInTx(context.Background(), dbConn, `insert into foo values (1, "one")`,
		dbLocked, time.Millisecond*busyTimeoutMs*2)

	dbConn2, err := sql.Open("sqlite3", dbDSN)
	require.NoError(t, err)
	defer func() { require.NoError(t, dbConn2.Close()) }()

	require.NoError(t, <-dbLocked)

	var attempts int
	var firstErr error
	backOffPolicy := retry.NewConstantBackoffPolicy(time.Millisecond*busyTimeoutMs/2, 10)
	require.NoError(t, retry.DoWithRetry(context.Background(), backOffPolicy, dbkit.GetIsRetryable(dbConn2.Driver()), nil, func(ctx context.Context) error {
		attempts++
		execErr := execInTx(ctx, dbConn2, `insert into foo values (2, "two")`)
		if firstErr == nil {
			firstErr = execErr
		}
		return execErr
	}))
	var sqliteErr sqlite3.Error
	require.ErrorAs(t, firstErr, &sqliteErr)
	require.ErrorIs(t, sqliteErr.Code, sqlite3.ErrBusy)
	require.Greater(t, attempts, 1)
}

func TestSqliteRetryOnBusyErrorTimedOut(t *testing.T) {
	const busyTimeoutMs = 100

	dbPath := t.TempDir() + "/TestSqliteRetryOnBusyErrorTimedOut.db"
	dbDSN := fmt.Sprintf("%s?_busy_timeout=%d", dbPath, busyTimeoutMs)

	dbConn, err := sql.Open("sqlite3", dbDSN)
	require.NoError(t, err)
	defer func() { require.NoError(t, dbConn.Close()) }()

	_, err = dbConn.Exec(createFooTable)
	require.NoError(t, err)

	dbLocked := make(chan error)
	go execAndSleepInTx(context.Background(), dbConn, `insert into foo values (1, "one")`,
		dbLocked, time.Millisecond*busyTimeoutMs*4)

	dbConn2, err := sql.Open("sqlite3", dbDSN)
	require.NoError(t, err)
	defer func() { require.NoError(t, dbConn2.Close()) }()

	require.NoError(t, <-dbLocked)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*busyTimeoutMs*2)
	defer cancel()

	var attempts int
	var firstErr error
	backOffPolicy := retry.NewConstantBackoffPolicy(time.Millisecond*busyTimeoutMs/2, 10)
	err = retry.DoWithRetry(ctx, backOffPolicy, dbkit.GetIsRetryable(dbConn2.Driver()), nil, func(ctx context.Context) error {
		attempts++
		execErr := execInTx(ctx, dbConn2, `insert into foo values (2, "two")`)
		if firstErr == nil {
			firstErr = execErr
		}
		return execErr
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	var sqliteErr sqlite3.Error
	require.ErrorAs(t, firstErr, &sqliteErr)
	require.ErrorIs(t, sqliteErr.Code, sqlite3.ErrBusy)
	require.Greater(t, attempts, 1)
}

func TestNoRetryOnOtherErrors(t *testing.T) {
	dbPath := t.TempDir() + "/TestTransactionError.db"

	dbConn, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, dbConn.Close()) }()

	var attempts int
	backOffPolicy := retry.NewConstantBackoffPolicy(time.Millisecond, 10)
	err = retry.DoWithRetry(context.Background(), backOffPolicy, dbkit.GetIsRetryable(dbConn.Driver()), nil, func(ctx context.Context) error {
		attempts++
		_, err = dbConn.Exec(`drop table foo`)
		return err
	})
	require.EqualError(t, err, "no such table: foo")
	require.Equal(t, 1, attempts)
}

func TestSqliteIsRetryable(t *testing.T) {
	isRetryable := dbkit.GetIsRetryable(&sqlite3.SQLiteDriver{})
	require.NotNil(t, isRetryable)
	require.True(t, isRetryable(sqlite3.Error{
		Code: sqlite3.ErrBusy,
	}))
	require.True(t, isRetryable(sqlite3.Error{
		Code: sqlite3.ErrLocked,
	}))
	require.False(t, isRetryable(driver.ErrBadConn))
	require.True(t, isRetryable(fmt.Errorf("wrapped error: %w", sqlite3.Error{
		Code: sqlite3.ErrBusy,
	})))
}

func execAndSleepInTx(ctx context.Context, dbConn *sql.DB, stmt string, errCh chan error, sleepTime time.Duration) {
	tx, txErr := dbConn.BeginTx(ctx, nil)
	if txErr != nil {
		errCh <- txErr
		return
	}
	if _, txErr = tx.Exec(stmt); txErr != nil {
		_ = tx.Rollback()
		errCh <- txErr
		return
	}
	errCh <- nil
	time.Sleep(sleepTime)
	_ = tx.Commit()
}

func execInTx(ctx context.Context, dbConn *sql.DB, stmt string) error {
	tr, err := dbConn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	_, err = tr.Exec(stmt)
	if err != nil {
		_ = tr.Rollback()
		return err
	}
	return tr.Commit()
}

func TestConfig(t *testing.T) {
	t.Run("read sqlite parameters", func(t *testing.T) {
		allDialects := []dbkit.Dialect{
			dbkit.DialectSQLite,
			dbkit.DialectMySQL,
			dbkit.DialectPostgres,
			dbkit.DialectMSSQL,
		}

		cfgData := bytes.NewBufferString(`
db:
  maxOpenConns: 20
  maxIdleConns: 10
  connMaxLifeTime: 1m
  dialect: sqlite3
  sqlite3:
    path: ":memory:"
`)
		cfg := dbkit.NewConfig(allDialects)
		err := config.NewDefaultLoader("").LoadFromReader(cfgData, config.DataTypeYAML, cfg)
		require.NoError(t, err)
		require.Equal(t, 20, cfg.MaxOpenConns)
		require.Equal(t, 10, cfg.MaxIdleConns)
		require.Equal(t, time.Minute, cfg.ConnMaxLifetime)
		require.Equal(t, dbkit.DialectSQLite, cfg.Dialect)
		require.Equal(t, ":memory:", cfg.SQLite.Path)
		require.Equal(t, sql.LevelDefault, cfg.TxIsolationLevel())
	})
}
