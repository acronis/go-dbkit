/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package distrlock

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	gotesting "testing"
	"time"

	"github.com/acronis/go-appkit/log/logtest"
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/acronis/go-dbkit"
	"github.com/acronis/go-dbkit/internal/testing"
	"github.com/acronis/go-dbkit/migrate"
	_ "github.com/acronis/go-dbkit/postgres"
)

func TestDBManager_Postgres(t *gotesting.T) {
	runDBManagerTests(t, dbkit.DialectPostgres)
}

func TestDBManager_Pgx(t *gotesting.T) {
	runDBManagerTests(t, dbkit.DialectPgx)
}

func TestDBManager_MySQL(t *gotesting.T) {
	runDBManagerTests(t, dbkit.DialectMySQL)
}

func TestDBLock_DoExclusively_Postgres(t *gotesting.T) {
	runDBLockDoExclusivelyTests(t, dbkit.DialectPostgres)
}

func TestDBLock_DoExclusively_MySQL(t *gotesting.T) {
	runDBLockDoExclusivelyTests(t, dbkit.DialectMySQL)
}

//nolint:gocyclo
func runDBManagerTests(t *gotesting.T, dialect dbkit.Dialect) {
	containerCtx, containerCtxClose := context.WithTimeout(context.Background(), time.Minute*2)
	defer containerCtxClose()

	dbConn, stop := testing.MustRunAndOpenTestDB(containerCtx, string(dialect))
	defer func() { require.NoError(t, stop(containerCtx)) }()

	dbManager, err := NewDBManager(dialect)
	require.NoError(t, err)

	migMngr, err := migrate.NewMigrationsManager(dbConn, dialect, logtest.NewLogger())
	require.NoError(t, err)
	require.NoError(t, migMngr.Run(dbManager.Migrations(), migrate.MigrationsDirectionUp))

	txLevels := []sql.IsolationLevel{
		sql.LevelReadUncommitted,
		sql.LevelReadCommitted,
		sql.LevelRepeatableRead,
		sql.LevelSerializable,
	}
	for i := range txLevels {
		txLevel := txLevels[i]
		testName := fmt.Sprintf(
			"attempt to acquire 2 locks with the same key within 2 different concurrent transactions, level=%q", txLevel)
		t.Run(testName, func(t *gotesting.T) {
			const ctxTimeout = 10 * time.Second
			const lockTimeout = 1 * time.Second
			const lock2CtxTimeout = 100 * time.Millisecond

			ctx, ctxCancel := context.WithTimeout(context.Background(), ctxTimeout)
			defer ctxCancel()

			lockKey := uuid.NewString()
			lock1, lock2 := makeTwoLocks(ctx, t, dbConn, dbManager, lockKey, lockKey)

			tx1, err := dbConn.BeginTx(ctx, &sql.TxOptions{Isolation: txLevel})
			require.NoError(t, err)
			defer func() {
				require.NoError(t, tx1.Commit())
			}()

			tx2, err := dbConn.BeginTx(ctx, &sql.TxOptions{Isolation: txLevel})
			require.NoError(t, err)
			defer assertRollbackWithCtxTimeoutError(t, dialect, tx2)()

			require.NoError(t, lock1.Acquire(ctx, tx1, lockTimeout))
			require.NotEmpty(t, lock1.Token())

			// The second acquire should be blocked (UPDATE leads to exclusive lock) and finally context timeout will be exceeded.
			lock2Ctx, lock2CtxCancel := context.WithTimeout(ctx, lock2CtxTimeout)
			defer lock2CtxCancel()
			acquireErr := lock2.Acquire(lock2Ctx, tx2, lockTimeout)
			require.Error(t, acquireErr)
			require.Empty(t, lock2.Token())
			switch dialect {
			case dbkit.DialectMySQL:
				require.ErrorIs(t, acquireErr, context.DeadlineExceeded)
			case dbkit.DialectPostgres:
				// In the Postgres' case "canceling statement due to user request" error will be returned
				// instead of context.DeadlineExceeded (pq "feature").
				require.ErrorContains(t, acquireErr, "canceling statement due to user request")
			case dbkit.DialectPgx:
				require.ErrorIs(t, acquireErr, context.DeadlineExceeded)
			}
		})
	}

	t.Run("acquire 2 locks with 2 different keys within 2 different concurrent transactions", func(t *gotesting.T) {
		const ctxTimeout = 10 * time.Second
		const lockTimeout = 1 * time.Second

		ctx, ctxCancel := context.WithTimeout(context.Background(), ctxTimeout)
		defer ctxCancel()

		lockKey1, lockKey2 := uuid.NewString(), uuid.NewString()
		lock1, lock2 := makeTwoLocks(ctx, t, dbConn, dbManager, lockKey1, lockKey2)

		tx1, err := dbConn.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, tx1.Commit())
		}()

		tx2, err := dbConn.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, tx2.Commit())
		}()

		require.NoError(t, lock1.Acquire(ctx, tx1, lockTimeout))
		require.NotEmpty(t, lock1.Token())

		require.NoError(t, lock2.Acquire(ctx, tx2, lockTimeout))
		require.NotEmpty(t, lock2.Token())
	})

	t.Run("attempt to acquire lock with the same key twice consequentially", func(t *gotesting.T) {
		const ctxTimeout = 10 * time.Second
		const lockTimeout = 1 * time.Second
		lockKey := uuid.NewString()

		ctx, ctxCancel := context.WithTimeout(context.Background(), ctxTimeout)
		defer ctxCancel()

		var lock DBLock
		require.NoError(t, dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) (err error) {
			lock, err = dbManager.NewLock(ctx, tx, lockKey)
			return err
		}))

		require.NoError(t, dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) error {
			return lock.Acquire(ctx, tx, lockTimeout)
		}))

		acquireErr := dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) error {
			return lock.Acquire(ctx, tx, lockTimeout)
		})
		require.Error(t, acquireErr)
		require.ErrorIs(t, acquireErr, ErrLockAlreadyAcquired)
	})

	t.Run("acquire lock, release it, and acquire again", func(t *gotesting.T) {
		const ctxTimeout = 10 * time.Second
		const lockTimeout = 1 * time.Second
		lockKey := uuid.NewString()

		ctx, ctxCancel := context.WithTimeout(context.Background(), ctxTimeout)
		defer ctxCancel()

		var lock DBLock
		require.NoError(t, dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) (err error) {
			lock, err = dbManager.NewLock(ctx, tx, lockKey)
			return err
		}))

		require.NoError(t, dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) error {
			return lock.Acquire(ctx, tx, lockTimeout)
		}))

		// It must be impossible to acquire not released lock twice.
		acquireErr := dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) error {
			return lock.Acquire(ctx, tx, lockTimeout)
		})
		require.Error(t, acquireErr)
		require.ErrorIs(t, acquireErr, ErrLockAlreadyAcquired)

		// However after unlock ...
		require.NoError(t, dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) error {
			return lock.Release(ctx, tx)
		}))

		// ... it must be possible to acquire the same lock at the second time.
		require.NoError(t, dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) error {
			return lock.Acquire(ctx, tx, lockTimeout)
		}))
	})

	t.Run("attempt to acquire lock with the same key many times concurrently", func(t *gotesting.T) {
		const locksNum = 10
		const ctxTimeout = 10 * time.Second
		const lockTimeout = 1 * time.Second
		lockKey := uuid.NewString()

		ctx, ctxCancel := context.WithTimeout(context.Background(), ctxTimeout)
		defer ctxCancel()

		locks := make([]DBLock, locksNum)
		for i := 0; i < locksNum; i++ {
			require.NoError(t, dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) (err error) {
				locks[i], err = dbManager.NewLock(ctx, tx, lockKey) //nolint:scopelint
				return err
			}))
		}

		var wg sync.WaitGroup
		errs := make(chan error, locksNum)
		for i := 0; i < locksNum; i++ {
			wg.Add(1)
			go func(lock DBLock) {
				defer wg.Done()
				errs <- dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) error {
					return lock.Acquire(ctx, tx, lockTimeout)
				})
			}(locks[i])
		}
		wg.Wait()
		close(errs)

		lockedCount := 0
		for err = range errs {
			if err == nil {
				lockedCount++
				continue
			}
			require.ErrorIs(t, err, ErrLockAlreadyAcquired)
		}
		require.Equal(t, 1, lockedCount)
	})

	t.Run("acquire and release locks with the same key many times concurrently", func(t *gotesting.T) {
		const locksNum = 10
		const ctxTimeout = 100 * time.Second
		const lockTimeout = 1 * time.Second
		lockKey := uuid.NewString()

		ctx, ctxCancel := context.WithTimeout(context.Background(), ctxTimeout)
		defer ctxCancel()

		locks := make([]DBLock, locksNum)
		for i := 0; i < locksNum; i++ {
			require.NoError(t, dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) (err error) {
				locks[i], err = dbManager.NewLock(ctx, tx, lockKey) //nolint:scopelint
				return err
			}))
		}

		var wg sync.WaitGroup
		var counter int32
		fatalErr := make(chan error)
		for i := 0; i < locksNum; i++ {
			wg.Add(1)
			go func(lock DBLock) {
				defer wg.Done()
				// Continuously trying to acquire the lock.
				for {
					acquireErr := dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) error {
						return lock.Acquire(ctx, tx, lockTimeout)
					})
					if acquireErr == nil {
						atomic.AddInt32(&counter, 1)
						break
					}
					if errors.Is(acquireErr, ErrLockAlreadyAcquired) {
						time.Sleep(time.Millisecond * 10)
						continue
					}
					if acquireErr != nil {
						select {
						case fatalErr <- acquireErr:
						default:
						}
						return
					}
				}

				// Release as soon as we got it locked.
				releaseErr := dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) error {
					return lock.Release(ctx, tx)
				})
				if releaseErr != nil {
					select {
					case fatalErr <- releaseErr:
					default:
					}
				}
			}(locks[i])
		}
		wg.Wait()
		require.EqualValues(t, locksNum, counter)
		close(fatalErr)
		require.NoError(t, <-fatalErr)
	})

	t.Run("attempt to release lock after timeout", func(t *gotesting.T) {
		const ctxTimeout = 10 * time.Second
		const lockTimeout = 10 * time.Millisecond
		lockKey := uuid.NewString()

		ctx, ctxCancel := context.WithTimeout(context.Background(), ctxTimeout)
		defer ctxCancel()

		var lock DBLock
		require.NoError(t, dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) (err error) {
			lock, err = dbManager.NewLock(ctx, tx, lockKey)
			if err != nil {
				return
			}
			return lock.Acquire(ctx, tx, lockTimeout)
		}))

		// wait for a timeout
		time.Sleep(lockTimeout * 2)

		releaseErr := dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) error {
			return lock.Release(ctx, tx)
		})
		require.ErrorIs(t, releaseErr, ErrLockAlreadyReleased)
	})

	t.Run("acquire with static token", func(t *gotesting.T) {
		const ctxTimeout = 10 * time.Second
		const lockTTL = 10 * time.Minute
		lockKey := uuid.NewString()
		token := uuid.NewString()

		ctx, ctxCancel := context.WithTimeout(context.Background(), ctxTimeout)
		defer ctxCancel()

		var lock DBLock
		require.NoError(t, dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) (err error) {
			lock, err = dbManager.NewLock(ctx, tx, lockKey)
			if err != nil {
				return
			}
			return lock.AcquireWithStaticToken(ctx, tx, token, lockTTL)
		}))

		// must be able to acquire the lock with the same token
		require.NoError(t, dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) (err error) {
			lock, err = dbManager.NewLock(ctx, tx, lockKey)
			if err != nil {
				return
			}
			return lock.AcquireWithStaticToken(ctx, tx, token, lockTTL)
		}))

		require.NoError(t, dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) error {
			return lock.Release(ctx, tx)
		}))

		require.NoError(t, dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) (err error) {
			lock, err = dbManager.NewLock(ctx, tx, lockKey)
			if err != nil {
				return
			}
			return lock.Acquire(ctx, tx, lockTTL)
		}))

		acquireErr := dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) error {
			return lock.AcquireWithStaticToken(ctx, tx, token, lockTTL)
		})
		require.ErrorIs(t, acquireErr, ErrLockAlreadyAcquired, "it must be impossible to acquire already acquired lock with different token")
	})

	t.Run(
		"attempt to acquire 2 locks with the same key within 2 different concurrent transaction, but all contexts are canceled",
		func(t *gotesting.T) {
			const ctxTimeout = 1 * time.Second // Context timeout should be the same as lock timeout for this test.
			const lockTimeout = 1 * time.Second

			ctx, ctxCancel := context.WithTimeout(context.Background(), ctxTimeout)
			defer ctxCancel()

			lockKey := uuid.NewString()
			lock1, lock2 := makeTwoLocks(ctx, t, dbConn, dbManager, lockKey, lockKey)

			tx1, txErr := dbConn.BeginTx(ctx, nil)
			require.NoError(t, txErr)
			defer assertRollbackWithCtxTimeoutError(t, dialect, tx1)()

			tx2, txErr := dbConn.BeginTx(ctx, nil)
			require.NoError(t, txErr)
			defer assertRollbackWithCtxTimeoutError(t, dialect, tx2)()

			require.NoError(t, lock1.Acquire(ctx, tx1, lockTimeout))
			require.NotEmpty(t, lock1.Token())

			acquireErr := lock2.Acquire(ctx, tx2, lockTimeout)
			require.Error(t, acquireErr)
			if dialect != dbkit.DialectPostgres {
				require.ErrorIs(t, acquireErr, context.DeadlineExceeded)
			} else {
				require.Truef(t,
					strings.Contains(acquireErr.Error(), "canceling statement due to user request") ||
						errors.Is(acquireErr, context.DeadlineExceeded),
					"unexpected error: %v", acquireErr)
			}
			require.Empty(t, lock2.Token())
		},
	)

	t.Run("lock extension", func(t *gotesting.T) {
		const ctxTimeout = 10 * time.Second
		const lockTimeout = 1 * time.Second
		lockKey := uuid.NewString()

		ctx, ctxCancel := context.WithTimeout(context.Background(), ctxTimeout)
		defer ctxCancel()

		var lock DBLock
		require.NoError(t, dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) (err error) {
			lock, err = dbManager.NewLock(ctx, tx, lockKey)
			if err != nil {
				return
			}
			return lock.Acquire(ctx, tx, lockTimeout)
		}))

		// Extend lock 3 times.
		for i := 0; i < 3; i++ {
			time.Sleep(lockTimeout / 2)
			require.NoError(t, dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) error {
				return lock.Extend(ctx, tx)
			}))
		}

		// Wait while lock will be released by timeout.
		time.Sleep(lockTimeout * 2)

		extendErr := dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) error {
			return lock.Extend(ctx, tx)
		})
		require.ErrorIs(t, extendErr, ErrLockAlreadyReleased)
	})
}

func runDBLockDoExclusivelyTests(t *gotesting.T, dialect dbkit.Dialect) {
	containerCtx, containerCtxClose := context.WithTimeout(context.Background(), time.Minute*2)
	defer containerCtxClose()

	dbConn, stop := testing.MustRunAndOpenTestDB(containerCtx, string(dialect))
	defer func() { require.NoError(t, stop(containerCtx)) }()

	dbManager, err := NewDBManager(dialect)
	require.NoError(t, err)

	migMngr, err := migrate.NewMigrationsManager(dbConn, dialect, logtest.NewLogger())
	require.NoError(t, err)
	require.NoError(t, migMngr.Run(dbManager.Migrations(), migrate.MigrationsDirectionUp))

	t.Run("lock is acquired with successful periodic extensions", func(t *gotesting.T) {
		ctx, ctxCancel := context.WithTimeout(context.Background(), time.Second*30)
		defer ctxCancel()

		const lockTTL = time.Second * 3
		const releaseTimeout = time.Second * 1
		const extendInterval = time.Second * 1

		lockKey := uuid.NewString()
		lock1, lock2 := makeTwoLocks(ctx, t, dbConn, dbManager, lockKey, lockKey)

		exJobStarted := make(chan struct{})
		exJobFinished := make(chan struct{})
		doExResult := make(chan error)
		go func() {
			doExResult <- lock1.DoExclusively(ctx, dbConn, lockTTL, extendInterval, releaseTimeout, logtest.NewLogger(), func(ctx context.Context) error {
				close(exJobStarted)
				<-exJobFinished // Wait for the job that should be executing exclusively.
				return nil
			})
		}()

		<-exJobStarted // Wait until the exclusive job is started.

		// New lock cannot be acquired since the same key is already locked by another goroutine.
		for i := 0; i < 7; i++ {
			err = lock2.DoExclusively(ctx, dbConn, lockTTL, extendInterval, releaseTimeout, logtest.NewLogger(), func(ctx context.Context) error {
				return nil
			})
			require.ErrorIs(t, err, ErrLockAlreadyAcquired)
			time.Sleep(time.Second)
		}

		close(exJobFinished)
		require.NoError(t, <-doExResult)
		time.Sleep(lockTTL * 2) // Wait until the lock is released.

		// Now a new lock can be acquired successfully.
		require.NoError(t, lock2.DoExclusively(ctx, dbConn, lockTTL, extendInterval, releaseTimeout, logtest.NewLogger(), func(ctx context.Context) error {
			return nil
		}))
	})

	t.Run("lock is acquired but periodic extension interval is too long", func(t *gotesting.T) {
		ctx, ctxCancel := context.WithTimeout(context.Background(), time.Second*30)
		defer ctxCancel()

		const lockTTL = time.Second * 3
		const releaseTimeout = time.Second * 1
		const periodicExtendInterval = time.Second * 10

		lockKey := uuid.NewString()
		lock1, lock2 := makeTwoLocks(ctx, t, dbConn, dbManager, lockKey, lockKey)

		exJobStarted := make(chan struct{})
		doExResult := make(chan error)
		go func() {
			doExResult <- lock1.DoExclusively(ctx, dbConn, lockTTL, periodicExtendInterval, releaseTimeout, logtest.NewLogger(), func(ctx context.Context) error {
				close(exJobStarted)
				<-ctx.Done() // The second goroutine should acquire lock since extension interval is too long.
				return ctx.Err()
			})
		}()

		<-exJobStarted // Wait until the exclusive job is started.

		time.Sleep(lockTTL * 2) // Wait until the lock is released.
		err = lock2.DoExclusively(ctx, dbConn, lockTTL, periodicExtendInterval, releaseTimeout, logtest.NewLogger(), func(ctx context.Context) error {
			return nil
		})
		require.NoError(t, err)

		// doExResult should contain the error since the first lock cannot be extended and context was canceled.
		require.EqualError(t, <-doExResult, context.Canceled.Error())
	})
}

func makeTwoLocks(
	ctx context.Context, t *gotesting.T, dbConn *sql.DB, dbManager *DBManager, key1, key2 string,
) (lock1, lock2 DBLock) {
	t.Helper()

	require.NoError(t, dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) (err error) {
		lock1, err = dbManager.NewLock(ctx, tx, key1)
		return err
	}))
	require.Equal(t, key1, lock1.Key)

	require.NoError(t, dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) (err error) {
		lock2, err = dbManager.NewLock(ctx, tx, key2)
		return err
	}))
	require.Equal(t, key2, lock2.Key)

	return
}

func assertRollbackWithCtxTimeoutError(t *gotesting.T, dialect dbkit.Dialect, tx *sql.Tx) func() {
	return func() {
		rollbackErr := tx.Rollback()
		var ok bool
		switch dialect {
		case dbkit.DialectMySQL:
			ok = assert.True(t, errors.Is(rollbackErr, sql.ErrTxDone) ||
				errors.Is(rollbackErr, mysql.ErrInvalidConn) ||
				rollbackErr == nil, // Rollback sometimes can return nil error in case of mysql driver .
			)
		case dbkit.DialectPostgres:
			ok = assert.True(t, errors.Is(rollbackErr, sql.ErrTxDone) ||
				errors.Is(rollbackErr, driver.ErrBadConn) ||
				strings.Contains(rollbackErr.Error(), "canceling statement due to user request"))
		case dbkit.DialectPgx:
			ok = assert.True(t, errors.Is(rollbackErr, sql.ErrTxDone) ||
				errors.Is(rollbackErr, context.DeadlineExceeded) ||
				strings.Contains(rollbackErr.Error(), "conn closed"), // Pgx may return `conn closed` error when context timeout exceeded.
			)
		}
		if !ok {
			t.Fatalf("unexpected error: %v", rollbackErr)
		}
	}
}
