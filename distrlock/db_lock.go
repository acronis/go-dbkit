/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package distrlock

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/acronis/go-appkit/log"
	"github.com/google/uuid"

	"github.com/acronis/go-dbkit"
	"github.com/acronis/go-dbkit/migrate"
)

const defaultTableName = "distributed_locks"

// DBManager provides management functionality for distributed locks based on the SQL database.
type DBManager struct {
	queries dbQueries
}

// DBManagerOpts represents an options for DBManager.
type DBManagerOpts struct {
	TableName string
}

// NewDBManager creates new distributed lock manager that uses SQL database as a backend.
func NewDBManager(dialect dbkit.Dialect) (*DBManager, error) {
	return NewDBManagerWithOpts(dialect, DBManagerOpts{TableName: defaultTableName})
}

// NewDBManagerWithOpts is a more configurable version of the NewDBManager.
func NewDBManagerWithOpts(dialect dbkit.Dialect, opts DBManagerOpts) (*DBManager, error) {
	q, err := newDBQueries(dialect, opts.TableName)
	if err != nil {
		return nil, err
	}
	return &DBManager{q}, nil
}

// Migrations returns set of migrations that must be applied before creating new locks.
func (m *DBManager) Migrations() []migrate.Migration {
	return []migrate.Migration{
		migrate.NewCustomMigration(
			createTableMigrationID,
			[]string{m.queries.createTable},
			[]string{m.queries.dropTable},
			nil,
			nil,
		),
	}
}

// NewLock creates new initialized (but not acquired) distributed lock.
func (m *DBManager) NewLock(ctx context.Context, executor sqlExecutor, key string) (DBLock, error) {
	if key == "" {
		return DBLock{}, fmt.Errorf("lock key cannot be empty")
	}
	if len(key) > 40 {
		return DBLock{}, fmt.Errorf("lock key cannot be longer than 40 symbols")
	}
	if _, err := executor.ExecContext(ctx, m.queries.initLock, key); err != nil {
		return DBLock{}, err
	}
	return DBLock{Key: key, manager: m}, nil
}

// DBLock represents a lock object in the database.
type DBLock struct {
	Key     string
	TTL     time.Duration
	token   string
	manager *DBManager
}

// Acquire acquires lock for the key in the database.
func (l *DBLock) Acquire(ctx context.Context, executor sqlExecutor, lockTTL time.Duration) error {
	return l.AcquireWithStaticToken(ctx, executor, uuid.NewString(), lockTTL)
}

// AcquireWithStaticToken acquires lock for the key in the database with a static token.
// There two use cases for this method:
//  1. When you need repeatably acquire the same lock preventing other processes from acquiring it at the same time.
//     As an example you can block old version of workers before the upgrade and starting new version of them.
//  2. When you need several processes to acquire the same lock.
//
// Please use Acquire instead of this method unless you have a good reason to use it.
func (l *DBLock) AcquireWithStaticToken(ctx context.Context, executor sqlExecutor, token string, lockTTL time.Duration) error {
	interval := l.manager.queries.intervalMaker(lockTTL)
	err := execQueryAndCheck(ctx, executor, l.manager.queries.acquireLock,
		[]interface{}{interval, token, l.Key, token}, ErrLockAlreadyAcquired)
	if err != nil {
		return err
	}
	l.TTL = lockTTL
	l.token = token
	return nil
}

// Release releases lock for the key in the database.
func (l *DBLock) Release(ctx context.Context, executor sqlExecutor) error {
	return execQueryAndCheck(ctx, executor,
		l.manager.queries.releaseLock, []interface{}{l.Key, l.token}, ErrLockAlreadyReleased)
}

// Extend resets expiration timeout for already acquired lock.
// ErrLockAlreadyReleased error will be returned if lock is already released, in this case lock should be acquired again.
func (l *DBLock) Extend(ctx context.Context, executor sqlExecutor) error {
	interval := l.manager.queries.intervalMaker(l.TTL)
	return execQueryAndCheck(ctx, executor,
		l.manager.queries.extendLock, []interface{}{interval, l.Key, l.token}, ErrLockAlreadyReleased)
}

// Token returns token of the last acquired lock.
// May be used in logs to make investigation process easier.
func (l *DBLock) Token() string {
	return l.token
}

// DoExclusively acquires distributed lock, starts a separate goroutine that periodical extends it and calls passed function.
// When function is finished, acquired lock is released.
func (l *DBLock) DoExclusively(
	ctx context.Context,
	dbConn *sql.DB,
	lockTTL time.Duration,
	periodicExtendInterval time.Duration,
	releaseTimeout time.Duration,
	logger log.FieldLogger,
	fn func(ctx context.Context) error,
) error {
	if acquireLockErr := dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) error {
		return l.Acquire(ctx, tx, lockTTL)
	}); acquireLockErr != nil {
		return acquireLockErr
	}

	logger = logger.With(log.String("distrlock_key", l.Key), log.String("distrlock_token", l.token))

	defer func() {
		// If the ctx is canceled, we should be able to release the lock.
		releaseCtx, releaseCtxCancel := context.WithTimeout(context.Background(), releaseTimeout)
		defer releaseCtxCancel()
		if releaseLockErr := dbkit.DoInTx(releaseCtx, dbConn, func(tx *sql.Tx) error {
			return l.Release(releaseCtx, tx)
		}); releaseLockErr != nil {
			logger.Error("failed to release db lock", log.Error(releaseLockErr))
		}
	}()

	newCtx, newCtxCancel := context.WithCancel(ctx)
	defer newCtxCancel()
	periodicalExtensionExit := make(chan struct{})
	periodicalExtensionDone := make(chan struct{})
	defer func() {
		close(periodicalExtensionDone)
		<-periodicalExtensionExit
	}()
	go func() {
		defer func() { close(periodicalExtensionExit) }()
		ticker := time.NewTicker(periodicExtendInterval)
		defer ticker.Stop()
		for {
			select {
			case <-periodicalExtensionDone:
				return
			case <-ticker.C:
				if extendLockErr := dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) error {
					return l.Extend(ctx, tx)
				}); extendLockErr != nil {
					logger.Error("failed to extend db lock", log.Error(extendLockErr))
					if errors.Is(extendLockErr, ErrLockAlreadyReleased) {
						newCtxCancel() // If lock was already released, let's try to stop exclusive job asap.
						return
					}
				}
			}
		}
	}()

	return fn(newCtx)
}

func execQueryAndCheck(ctx context.Context, executor sqlExecutor, query string, args []interface{}, errOnNoAffectedRows error) error {
	result, err := executor.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	// If the same context object is used in BeginTx() and in ExecContext() methods and it's canceled,
	// "context deadline exceeded" or "canceling statement due to user request" errors are not returned from the ExecContext().
	// This issue is actual for github.com/lib/pq driver (https://github.com/lib/pq/issues/874).
	// Probably it's because when a context is canceled, tx is rolled backed and this behavior is not handled properly in lib/pq.
	// We can apply a simple work around here and just check ctx.Err() as guys from cocroachdb did
	// (https://github.com/cockroachdb/cockroach/pull/39525/files#diff-f3aa9f413e52eca7d64bf33c9493ec426a0c54aa4dca7a9d948721aa365e96c0).
	// We have a separate sub-test for this case ("all contexts are canceled" in suffix).
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return errOnNoAffectedRows
	}
	return nil
}

type dbQueries struct {
	createTable   string
	dropTable     string
	initLock      string
	acquireLock   string
	releaseLock   string
	extendLock    string
	intervalMaker func(interval time.Duration) string
}

func newDBQueries(dialect dbkit.Dialect, tableName string) (dbQueries, error) {
	switch dialect {
	case dbkit.DialectPostgres, dbkit.DialectPgx:
		return dbQueries{
			createTable:   fmt.Sprintf(postgresCreateTableQuery, tableName),
			dropTable:     fmt.Sprintf(postgresDropTableQuery, tableName),
			initLock:      fmt.Sprintf(postgresInitLockQuery, tableName),
			acquireLock:   fmt.Sprintf(postgresAcquireLockQuery, tableName),
			releaseLock:   fmt.Sprintf(postgresReleaseLockQuery, tableName),
			extendLock:    fmt.Sprintf(postgresExtendLockQuery, tableName),
			intervalMaker: postgresMakeInterval,
		}, nil
	case dbkit.DialectMySQL:
		return dbQueries{
			createTable:   fmt.Sprintf(mySQLCreateTableQuery, tableName),
			dropTable:     fmt.Sprintf(mySQLDropTableQuery, tableName),
			initLock:      fmt.Sprintf(mySQLInitLockQuery, tableName),
			acquireLock:   fmt.Sprintf(mySQLAcquireLockQuery, tableName),
			releaseLock:   fmt.Sprintf(mySQLReleaseLockQuery, tableName),
			extendLock:    fmt.Sprintf(mySQLExtendLockQuery, tableName),
			intervalMaker: mySQLMakeInterval,
		}, nil
	default:
		return dbQueries{}, fmt.Errorf("unsupported sql dialect %q", dialect)
	}
}

type sqlExecutor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

const createTableMigrationID = "distrlock_00001_create_table"

//nolint:lll
const (
	postgresCreateTableQuery = `CREATE TABLE "%s" (lock_key varchar(40) PRIMARY KEY, token uuid, expire_at timestamp);`
	postgresDropTableQuery   = `DROP TABLE IF EXISTS "%s";`
	postgresInitLockQuery    = `INSERT INTO "%s" (lock_key) VALUES ($1) ON CONFLICT (lock_key) DO NOTHING;`
	postgresAcquireLockQuery = `UPDATE "%s" SET expire_at = NOW() + $1::interval, token = $2 WHERE lock_key = $3 AND ((expire_at IS NULL OR expire_at < NOW()) OR token = $4);`
	postgresReleaseLockQuery = `UPDATE "%s" SET expire_at = NULL WHERE lock_key = $1 AND token = $2 AND expire_at >= NOW();`
	postgresExtendLockQuery  = `UPDATE "%s" SET expire_at = NOW() + $1::interval WHERE lock_key = $2 AND token = $3 AND expire_at >= NOW();`
)

func postgresMakeInterval(interval time.Duration) string {
	return fmt.Sprintf("%d microseconds", interval.Microseconds())
}

//nolint:lll
const (
	mySQLCreateTableQuery = "CREATE TABLE `%s` (lock_key VARCHAR(40) PRIMARY KEY, token VARCHAR(36), expire_at BIGINT);"
	mySQLDropTableQuery   = "DROP TABLE IF EXISTS `%s`;"
	mySQLInitLockQuery    = "INSERT IGNORE `%s` (lock_key) VALUES (?);"
	mySQLAcquireLockQuery = "UPDATE `%s` SET expire_at = UNIX_TIMESTAMP(DATE_ADD(CURTIME(4), INTERVAL ? MICROSECOND))*10000, token = ? WHERE lock_key = ? AND ((expire_at IS NULL OR expire_at < UNIX_TIMESTAMP(CURTIME(4))*10000) OR token = ?);"
	mySQLReleaseLockQuery = "UPDATE `%s` SET expire_at = NULL WHERE lock_key = ? AND token = ? AND expire_at >= UNIX_TIMESTAMP(CURTIME(4))*10000;"
	mySQLExtendLockQuery  = "UPDATE `%s` SET expire_at = UNIX_TIMESTAMP(DATE_ADD(CURTIME(4), INTERVAL ? MICROSECOND))*10000 WHERE lock_key = ? AND token = ? AND expire_at >= UNIX_TIMESTAMP(CURTIME(4))*10000;"
)

func mySQLMakeInterval(interval time.Duration) string {
	return fmt.Sprintf("%d", interval.Microseconds())
}
