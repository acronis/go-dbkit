# distrlock

[![GoDoc Widget]][GoDoc]

`distrlock` is a Go package that implements distributed locking using SQL databases.
It allows multiple processes or services to coordinate access to shared resources by acquiring and releasing locks stored in a database.

### Features
- Distributed lock management using SQL databases (PostgreSQL, MySQL are supported now).
- Support for acquiring, releasing, and extending locks.
- Configurable lock expiration times.

## How It Works

`distrlock` uses a relational database to implement distributed locking. When a process acquires a lock, a record is inserted or updated in a designated table within the database. The lock entry includes a unique key, a token for verification, and an expiration time to handle failures or crashes. Other processes attempting to acquire the same lock must wait until it is released or expires. If required, the lock can be extended before expiration to prevent unintended release.

This approach ensures reliable concurrency control without requiring an external distributed coordination system like Zookeeper or etcd, making it lightweight and easy to integrate into existing systems that already use SQL databases.

## Usage

`distlock` provides a simple API for acquiring and releasing locks.

The following basic example demonstrates how to use `distrlock` to ensure exclusive execution of a critical section of code:

```go
package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"time"

	"github.com/acronis/go-dbkit"
	"github.com/acronis/go-dbkit/distrlock"
)

func main() {
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
}
```

If you need more customization or/and control over the lock lifecycle, you can use `DBManager` and `DBLock` objects directly:

```go
package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"time"

	"github.com/acronis/go-dbkit"
	"github.com/acronis/go-dbkit/distrlock"
)

func main() {
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
			log.Fatal(err)
		}
	}()

	time.Sleep(11 * time.Second) // Simulate work
}
```

## License

Copyright © 2024-2025 Acronis International GmbH.

Licensed under [MIT License](./../LICENSE).

[GoDoc]: https://pkg.go.dev/github.com/acronis/go-dbkit/distrlock
[GoDoc Widget]: https://godoc.org/github.com/acronis/go-dbkit/distrlock?status.svg