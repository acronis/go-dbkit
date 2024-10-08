/*
Copyright © 2019-2023 Acronis International GmbH.
*/

package testing

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/acronis/go-dbkit"
)

// DeadlockTest is internal function to simulate DB deadlock
func DeadlockTest(t *testing.T, dialect dbkit.Dialect, checkDeadlockErr func(err error) bool) {
	ctx, ctxCancel := context.WithTimeout(context.Background(), time.Second*30)
	defer ctxCancel()
	dbConn, stop := MustRunAndOpenTestDB(ctx, string(dialect))
	defer func() { require.NoError(t, stop(ctx)) }()

	table1Name := fmt.Sprintf("%s_deadlock_test1", dialect)
	table2Name := fmt.Sprintf("%s_deadlock_test2", dialect)

	tErr := createTables(ctx, dbConn, table2Name, table1Name)
	require.NoError(t, tErr)

	defer func() {
		err := cleanupDB(ctx, dbConn, table1Name, table2Name)
		require.NoError(t, err)
	}()

	var tran1Lock sync.WaitGroup
	var tran2Lock sync.WaitGroup
	var done sync.WaitGroup

	tran1Lock.Add(1)
	tran2Lock.Add(1)
	done.Add(1)

	var tx1Err, tx2Err error

	txOpts := &sql.TxOptions{Isolation: sql.LevelReadCommitted}
	go func(ctx context.Context) {
		defer done.Done()

		tx1Err = dbkit.DoInTxWithOpts(ctx, dbConn, txOpts, func(tx *sql.Tx) error {
			if _, err := tx.Exec(fmt.Sprintf("UPDATE %s SET name=$1 WHERE id=$2", table1Name), "test100", 1); err != nil {
				return err
			}
			tran1Lock.Done()
			tran2Lock.Wait()
			if _, err := tx.Exec(fmt.Sprintf("UPDATE %s SET name=$1 WHERE id=$2", table2Name), "test100", 1); err != nil {
				return err
			}
			return nil
		})
	}(ctx)

	done.Add(1)
	go func(ctx context.Context) {
		defer done.Done()
		tx2Err = dbkit.DoInTxWithOpts(ctx, dbConn, txOpts, func(tx *sql.Tx) error {
			if _, err := tx.Exec(fmt.Sprintf("UPDATE %s SET name=$1 WHERE id=$2", table2Name), "test100", 1); err != nil {
				return err
			}

			tran2Lock.Done()
			tran1Lock.Wait()

			if _, err := tx.Exec(fmt.Sprintf("UPDATE %s SET name=$1 WHERE id=$2", table1Name), "test100", 1); err != nil {
				return err
			}

			return nil
		})
	}(ctx)

	done.Wait()

	if tx1Err != nil {
		require.Truef(t, checkDeadlockErr(tx1Err), "Wrong error: %w", tx2Err)
		return
	}
	if tx2Err != nil {
		require.Truef(t, checkDeadlockErr(tx2Err), "Wrong error: %w", tx2Err)
		return
	}
	assert.Fail(t, "Deadlock error is expecting at one of the goroutines")
}

func cleanupDB(ctx context.Context, dbConn *sql.DB, table1Name string, table2Name string) error {
	return dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) error {
		if _, err := tx.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s;", table1Name)); err != nil {
			return err
		}
		if _, err := tx.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s;", table2Name)); err != nil {
			return err
		}
		return nil
	})
}

func createTables(ctx context.Context, dbConn *sql.DB, table2Name string, table1Name string) error {
	tErr := dbkit.DoInTx(ctx, dbConn, func(tx *sql.Tx) error {
		_, err := tx.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL);", table2Name))
		if err != nil {
			return err
		}

		_, err = tx.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL);", table1Name))
		if err != nil {
			return err
		}

		for i := 1; i != 3; i++ {
			name := fmt.Sprintf("test%d", i)
			_, err = tx.Exec(fmt.Sprintf("INSERT INTO %s(id, name) values ($1, $2)", table1Name), i, name)
			if err != nil {
				return err
			}
			_, err = tx.Exec(fmt.Sprintf("INSERT INTO %s(id, name) values ($1, $2)", table2Name), i, name)
			if err != nil {
				return err
			}
		}

		return nil
	})
	return tErr
}
