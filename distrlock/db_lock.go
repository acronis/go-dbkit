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
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/acronis/go-dbkit"
	"github.com/acronis/go-dbkit/migrate"
)

// DefaultTableName is a default name for the table that stores distributed locks.
const DefaultTableName = "distributed_locks"

// DBManager provides management functionality for distributed locks based on the SQL database.
type DBManager struct {
	queries dbQueries
}

// DBManagerOption is an option for NewDBManager.
type DBManagerOption func(*dbManagerOptions)

type dbManagerOptions struct {
	tableName string
}

// WithTableName sets a custom table name for the table that stores distributed locks.
func WithTableName(tableName string) DBManagerOption {
	return func(o *dbManagerOptions) {
		o.tableName = tableName
	}
}

// NewDBManager creates a new distributed lock manager that uses SQL database as a backend.
func NewDBManager(dialect dbkit.Dialect, options ...DBManagerOption) (*DBManager, error) {
	var opts dbManagerOptions
	for _, opt := range options {
		opt(&opts)
	}
	if opts.tableName == "" {
		opts.tableName = DefaultTableName
	}
	q, err := newDBQueries(dialect, opts.tableName)
	if err != nil {
		return nil, err
	}
	return &DBManager{q}, nil
}

// Migrations returns set of migrations that must be applied before creating new locks.
func (m *DBManager) Migrations() []migrate.Migration {
	return []migrate.Migration{
		migrate.NewCustomMigration(createTableMigrationID,
			[]string{m.CreateTableSQL()}, []string{m.DropTableSQL()}, nil, nil),
	}
}

// CreateTableSQL returns SQL query for creating a table that stores distributed locks.
func (m *DBManager) CreateTableSQL() string {
	return m.queries.createTable
}

// DropTableSQL returns SQL query for dropping a table that stores distributed locks.
func (m *DBManager) DropTableSQL() string {
	return m.queries.dropTable
}

// NewLock creates new initialized (but not acquired) distributed lock.
func (m *DBManager) NewLock(ctx context.Context, executor SQLExecutor, key string) (DBLock, error) {
	if key == "" {
		return DBLock{}, fmt.Errorf("lock key cannot be empty")
	}
	if len(key) > 40 {
		return DBLock{}, fmt.Errorf("lock key cannot be longer than 40 symbols")
	}
	if _, err := executor.ExecContext(ctx, m.queries.initLock, key); err != nil {
		return DBLock{}, fmt.Errorf("init lock with key %s: %w", key, err)
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
func (l *DBLock) Acquire(ctx context.Context, executor SQLExecutor, lockTTL time.Duration) error {
	return l.AcquireWithStaticToken(ctx, executor, uuid.NewString(), lockTTL)
}

// AcquireWithStaticToken acquires lock for the key in the database with a static token.
//
// There two use cases for this method:
//  1. When you need to repeatably acquire the same lock preventing other processes from acquiring it at the same time.
//     As an example, you can block an old version of workers before the upgrade and start a new version of them.
//  2. When you need several processes to acquire the same lock.
//
// Please use Acquire instead of this method unless you have a good reason to use it.
func (l *DBLock) AcquireWithStaticToken(ctx context.Context, executor SQLExecutor, token string, lockTTL time.Duration) error {
	interval := l.manager.queries.intervalMaker(lockTTL)
	err := execQueryAndCheckAffectedRow(ctx, executor, l.manager.queries.acquireLock,
		[]interface{}{interval, token, l.Key, token}, ErrLockAlreadyAcquired)
	if err != nil {
		return err
	}
	l.TTL = lockTTL
	l.token = token
	return nil
}

// Release releases lock for the key in the database.
func (l *DBLock) Release(ctx context.Context, executor SQLExecutor) error {
	return execQueryAndCheckAffectedRow(ctx, executor,
		l.manager.queries.releaseLock, []interface{}{l.Key, l.token}, ErrLockAlreadyReleased)
}

// Extend resets expiration timeout for already acquired lock.
// ErrLockAlreadyReleased error will be returned if lock is already released, in this case lock should be acquired again.
func (l *DBLock) Extend(ctx context.Context, executor SQLExecutor) error {
	interval := l.manager.queries.intervalMaker(l.TTL)
	return execQueryAndCheckAffectedRow(ctx, executor,
		l.manager.queries.extendLock, []interface{}{interval, l.Key, l.token}, ErrLockAlreadyReleased)
}

// Token returns token of the last acquired lock.
// May be used in logs to make the investigation process easier.
func (l *DBLock) Token() string {
	return l.token
}

// Logger is an interface for logging errors.
type Logger interface {
	Errorf(format string, args ...interface{})
}

type doOptions struct {
	lockTTL                time.Duration
	periodicExtendInterval time.Duration
	releaseTimeout         time.Duration
	logger                 Logger
}

// DoOption is an option for DoExclusively method.
type DoOption func(*doOptions)

// WithLockTTL sets TTL for the lock acquired by DoExclusively.
func WithLockTTL(ttl time.Duration) DoOption {
	return func(o *doOptions) {
		o.lockTTL = ttl
	}
}

// WithPeriodicExtendInterval sets interval for periodic lock extension.
func WithPeriodicExtendInterval(interval time.Duration) DoOption {
	return func(o *doOptions) {
		o.periodicExtendInterval = interval
	}
}

// WithReleaseTimeout sets timeout for lock release.
func WithReleaseTimeout(timeout time.Duration) DoOption {
	return func(o *doOptions) {
		o.releaseTimeout = timeout
	}
}

// WithLogger sets logger for DoExclusively.
func WithLogger(logger Logger) DoOption {
	return func(o *doOptions) {
		o.logger = logger
	}
}

// DoExclusively acquires distributed lock, calls passed function and releases the lock when the function is finished.
// Lock is acquired with a default TTL of 1 minute. TTL can be configured with WithLockTTL option.
// Additionally, the lock is extended periodically within a separate goroutine.
// Extension interval can be configured with WithPeriodicExtendInterval option. By default, it's half of the lock TTL.
// When the function is finished, acquired lock is released.
// Timeout for lock release can be configured with WithReleaseTimeout option. By default, it's 5 seconds.
func (l *DBLock) DoExclusively(
	ctx context.Context,
	dbConn *sql.DB,
	fn func(ctx context.Context) error,
	options ...DoOption,
) error {
	var opts doOptions
	for _, opt := range options {
		opt(&opts)
	}
	if opts.lockTTL == 0 {
		opts.lockTTL = 1 * time.Minute
	}
	if opts.periodicExtendInterval == 0 {
		opts.periodicExtendInterval = opts.lockTTL / 2
	}
	if opts.releaseTimeout == 0 {
		opts.releaseTimeout = 5 * time.Second
	}
	if opts.logger == nil {
		opts.logger = disabledLogger{}
	}

	if acquireLockErr := dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) error {
		return l.Acquire(ctx, tx, opts.lockTTL)
	}); acquireLockErr != nil {
		return acquireLockErr
	}

	defer func() {
		// If the ctx is canceled, we should be able to release the lock.
		releaseCtx, releaseCtxCancel := context.WithTimeout(context.Background(), opts.releaseTimeout)
		defer releaseCtxCancel()
		if releaseLockErr := dbkit.DoInTx(releaseCtx, dbConn, func(tx *sql.Tx) error {
			return l.Release(releaseCtx, tx)
		}); releaseLockErr != nil {
			opts.logger.Errorf("failed to release lock with key %s and token %s, error: %v", l.Key, l.token, releaseLockErr)
		}
	}()

	childCtx, childCtxCancel := context.WithCancel(ctx)
	defer childCtxCancel()

	periodicalExtensionExit := make(chan struct{})
	periodicalExtensionDone := make(chan struct{})
	defer func() {
		close(periodicalExtensionDone)
		<-periodicalExtensionExit
	}()

	go func() {
		defer func() { close(periodicalExtensionExit) }()
		ticker := time.NewTicker(opts.periodicExtendInterval)
		defer ticker.Stop()
		for {
			select {
			case <-periodicalExtensionDone:
				return
			case <-ticker.C:
				if extendErr := dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) error {
					return l.Extend(ctx, tx)
				}); extendErr != nil {
					opts.logger.Errorf("failed to extend lock with key %s and token %s, error: %v", l.Key, l.token, extendErr)
					if errors.Is(extendErr, ErrLockAlreadyReleased) {
						childCtxCancel() // If lock was already released, let's try to stop an exclusive job asap.
						return
					}
				}
			}
		}
	}()

	return fn(childCtx)
}

// CreateTableSQL returns SQL query for creating a table that stores distributed locks.
// DefaultTableName is used for the table name. If you need to use a custom table name, construct DBManager and DBLock manually instead.
func CreateTableSQL(dialect dbkit.Dialect) (string, error) {
	q, err := newDBQueries(dialect, DefaultTableName)
	if err != nil {
		return "", err
	}
	return q.createTable, nil
}

// DropTableSQL returns SQL query for dropping a table that stores distributed locks.
// DefaultTableName is used for the table name. If you need to use a custom table name, construct DBManager and DBLock manually instead.
func DropTableSQL(dialect dbkit.Dialect) (string, error) {
	q, err := newDBQueries(dialect, DefaultTableName)
	if err != nil {
		return "", err
	}
	return q.dropTable, nil
}

// DoExclusively acquires distributed lock, calls passed function and releases the lock when the function is finished.
// It's a ready-to-use helper function that creates a new DBManager, initializes a lock with the given key, and calls DoExclusively on it.
// DefaultTableName is used for the table name. If you need to use a custom table name, construct DBManager and DBLock manually instead.
// See DBLock.DoExclusively for more details.
func DoExclusively(
	ctx context.Context,
	dbConn *sql.DB,
	dbDialect dbkit.Dialect,
	key string,
	fn func(ctx context.Context) error,
	options ...DoOption,
) error {
	manager, err := NewDBManager(dbDialect)
	if err != nil {
		return fmt.Errorf("create DB manager: %w", err)
	}
	lock, err := manager.NewLock(ctx, dbConn, key)
	if err != nil {
		return fmt.Errorf("create new lock: %w", err)
	}
	return lock.DoExclusively(ctx, dbConn, fn, options...)
}

func execQueryAndCheckAffectedRow(
	ctx context.Context, executor SQLExecutor, query string, args []interface{}, errOnNoAffectedRows error,
) error {
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

	var affected int64
	if affected, err = result.RowsAffected(); err != nil {
		return err
	} else if affected == 0 {
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

type SQLExecutor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

const createTableMigrationID = "distrlock_00001_create_table"

//nolint:lll // SQL queries are more readable on single lines
const (
	postgresCreateTableQuery = `CREATE TABLE IF NOT EXISTS "%s" (lock_key varchar(40) PRIMARY KEY, token uuid, expire_at timestamp);`
	postgresDropTableQuery   = `DROP TABLE IF EXISTS "%s";`
	postgresInitLockQuery    = `INSERT INTO "%s" (lock_key) VALUES ($1) ON CONFLICT (lock_key) DO NOTHING;`
	postgresAcquireLockQuery = `UPDATE "%s" SET expire_at = NOW() + $1::interval, token = $2 WHERE lock_key = $3 AND ((expire_at IS NULL OR expire_at < NOW()) OR token = $4);`
	postgresReleaseLockQuery = `UPDATE "%s" SET expire_at = NULL WHERE lock_key = $1 AND token = $2 AND expire_at >= NOW();`
	postgresExtendLockQuery  = `UPDATE "%s" SET expire_at = NOW() + $1::interval WHERE lock_key = $2 AND token = $3 AND expire_at >= NOW();`
)

func postgresMakeInterval(interval time.Duration) string {
	return strconv.FormatInt(interval.Microseconds(), 10) + " microseconds"
}

//nolint:lll // SQL queries are more readable on single lines
const (
	mySQLCreateTableQuery = "CREATE TABLE IF NOT EXISTS `%s` (lock_key VARCHAR(40) PRIMARY KEY, token VARCHAR(36), expire_at BIGINT);"
	mySQLDropTableQuery   = "DROP TABLE IF EXISTS `%s`;"
	mySQLInitLockQuery    = "INSERT IGNORE `%s` (lock_key) VALUES (?);"
	mySQLAcquireLockQuery = "UPDATE `%s` SET expire_at = UNIX_TIMESTAMP(DATE_ADD(CURTIME(4), INTERVAL ? MICROSECOND))*10000, token = ? WHERE lock_key = ? AND ((expire_at IS NULL OR expire_at < UNIX_TIMESTAMP(CURTIME(4))*10000) OR token = ?);"
	mySQLReleaseLockQuery = "UPDATE `%s` SET expire_at = NULL WHERE lock_key = ? AND token = ? AND expire_at >= UNIX_TIMESTAMP(CURTIME(4))*10000;"
	mySQLExtendLockQuery  = "UPDATE `%s` SET expire_at = UNIX_TIMESTAMP(DATE_ADD(CURTIME(4), INTERVAL ? MICROSECOND))*10000 WHERE lock_key = ? AND token = ? AND expire_at >= UNIX_TIMESTAMP(CURTIME(4))*10000;"
)

func mySQLMakeInterval(interval time.Duration) string {
	return strconv.FormatInt(interval.Microseconds(), 10)
}

type disabledLogger struct{}

func (disabledLogger) Errorf(msg string, args ...interface{}) {}
