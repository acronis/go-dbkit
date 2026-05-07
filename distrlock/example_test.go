/*
Copyright © 2025 Acronis International GmbH.

Released under MIT license.
*/

package distrlock_test

import (
	"context"
	"database/sql"
	"log"
	"os"
	"strings"
	"time"

	"github.com/acronis/go-dbkit"
	"github.com/acronis/go-dbkit/distrlock"
)

func ExampleDoExclusively() {
	// Setup database connection
	db, err := sql.Open("mysql", os.Getenv("MYSQL_DSN"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create "distributed_locks" table for locks.
	createTableSQL, err := distrlock.CreateTableSQL(dbkit.DialectMySQL)
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.ExecContext(ctx, createTableSQL)
	if err != nil {
		log.Fatal(err)
	}

	// Do some work exclusively.
	// Unique key that will be used to ensure exclusive execution among multiple instances
	const lockKey = "test-lock-key-1"
	err = distrlock.DoExclusively(ctx, db, dbkit.DialectMySQL, lockKey,
		func(ctx context.Context) error {
			time.Sleep(10 * time.Second) // Simulate work.
			return nil
		})
	if err != nil {
		log.Fatal(err)
	}

	// Output:
}

func ExampleNewDBManager() {
	// Setup database connection
	db, err := sql.Open("mysql", os.Getenv("MYSQL_DSN"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create DBManager
	lockManager, err := distrlock.NewDBManager(dbkit.DialectMySQL,
		distrlock.WithTableName("my_distributed_locks"))
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// Create table for locks.
	_, err = db.ExecContext(ctx, lockManager.CreateTableSQL())
	if err != nil {
		log.Fatal(err)
	}

	// Unique key that will be used to ensure exclusive execution among multiple instances
	const lockKey = "test-lock-key-2"

	// Create lock.
	lock, err := lockManager.NewLock(ctx, db, lockKey)
	if err != nil {
		log.Fatal(err)
	}

	// Acquire lock, do some work and release lock.
	const lockTTL = 10 * time.Second
	if err = lock.Acquire(ctx, db, lockTTL); err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = lock.Release(ctx, db); err != nil {
			if strings.Contains(err.Error(), "distributed lock already released") {
				// Output comparison: redirect log output to stdout and disable
				// timestamps for stable output
				logger := log.New(os.Stdout, "", 0)
				logger.Println("distributed lock already released")
				return
			}
			log.Fatal(err)
		}
	}()

	time.Sleep(11 * time.Second) // Simulate work

	// Output:
	// distributed lock already released
}
