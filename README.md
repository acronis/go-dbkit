# Toolkit for working with SQL databases in Go

[![GoDoc Widget]][GoDoc]

`go‑dbkit` is a Go library designed to simplify and streamline working with SQL databases.
It provides a solid foundation for connection management, instrumentation, error retry mechanisms, and transaction handling.
Additionally, `go‑dbkit` offers a suite of specialized sub‑packages that address common database challenges—such as [distributed locking](./distrlock), [schema migrations](./migrate), and query builder utilities - making it a one‑stop solution for applications that needs to interact with multiple SQL databases.

## Features
- **Transaction Management**: Execute functions within transactions that automatically commit on success or roll back on error. The transaction runner abstracts the boilerplate, ensuring cleaner and more reliable code.
- **Retryable Queries**: Built‑in support for detecting and automatically retrying transient errors (e.g., deadlocks, lock timeouts) across various databases.
- **Distributed Locking**: Implement SQL‑based distributed locks to coordinate exclusive access to shared resources across multiple processes.
- **Database Migrations**: Seamlessly manage schema changes with support for both embedded SQL migrations (using Go’s embed package) and programmatic migration definitions.
- **Query Builder Utilities**: Enhance your query‐building experience with utilities for popular libraries:
  * [dbrutil](./dbrutil): Simplifies working with the [dbr query builder](https://github.com/gocraft/dbr), adding instrumentation (Prometheus metrics, slow query logging) and transaction support.
  * [goquutil](./goquutil): Provides helper routines for the [goqu query builder](https://github.com/doug-martin/goqu) (currently, no dedicated README exists—refer to the source for usage).

## Packages Overview
- Root `go‑dbkit` package provides configuration management, DSN generation, and the foundational retryable query functionality used across the library.
- [dbrutil](./dbrutil) offers utilities for the dbr query builder, including:
  * Instrumented connection opening with Prometheus metrics.
  *	Automatic slow query logging based on configurable thresholds.
  *	A transaction runner that simplifies commit/rollback logic.
  Read more in [dbrutil/README.md](./dbrutil/README.md).
- [distrlock](./distrlock) implements a lightweight, SQL‑based distributed locking mechanism that ensures exclusive execution of critical sections across concurrent services.
  Read more in [distrlock/README.md](./distrlock/README.md).
- [migrate](./migrate):
  Manage your database schema changes effortlessly with support for both embedded SQL files and programmatic migrations.
  Read more in [migrate/README.md](./migration/README.md).
- [goquutil](./goquutil) provides helper functions for working with the goqu query builder, streamlining common operations. (This package does not have its own README yet, so please refer to the source code for more details.)
- RDBMS‑Specific dedicated sub‑packages are provided for various relational databases:
  * [mysql](./mysql) includes DSN generation, retryable error handling, and other MySQL‑specific utilities.
  * [sqlite](./sqlite) contains helpers to integrate SQLite seamlessly into your projects.
  * [postgres](./postgres) & [pgx](./pgx) offers tools and error handling improvements for PostgreSQL using both the lib/pq and pgx drivers.
  * [mssql](./mssql) provides MSSQL‑specific error handling, including registration of retryable functions for deadlocks and related transient errors.
  Each of these packages registers its own retryable function in the init() block, ensuring that transient errors (like deadlocks or cached plan invalidations) are automatically retried.o

## Installation

```
go get -u github.com/acronis/go-dbkit
```

## Usage

### Basic Example

Below is a simple example that demonstrates how to register a retryable function for a MySQL database connection and execute a query within a transaction with a custom retry policy on transient errors.

```go
package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"time"

	"github.com/acronis/go-appkit/retry"

	"github.com/acronis/go-dbkit"

	// Import the `mysql` package for registering the retryable function for MySQL transient
	// errors (like deadlocks).
	_ "github.com/acronis/go-dbkit/mysql"
)

func main() {
	// Configure the database using the dbkit.Config struct.
	// In this example, we're using MySQL. Adjust Dialect and config fields for your target DB.
	cfg := &dbkit.Config{
		Dialect: dbkit.DialectMySQL,
		MySQL: dbkit.MySQLConfig{
			Host:     os.Getenv("MYSQL_HOST"),
			Port:     3306,
			User:     os.Getenv("MYSQL_USER"),
			Password: os.Getenv("MYSQL_PASSWORD"),
			Database: os.Getenv("MYSQL_DATABASE"),
		},
		MaxOpenConns: 16,
		MaxIdleConns: 8,
	}

	// Open the database connection.
	// The 2nd parameter is a boolean that indicates whether to ping the database.
	db, err := dbkit.Open(cfg, true)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Execute a transaction with a custom retry policy (exponential backoff with 3 retries,
	// starting from 10ms).
	retryPolicy := retry.NewConstantBackoffPolicy(10*time.Millisecond, 3)
	if err = dbkit.DoInTx(context.Background(), db, func(tx *sql.Tx) error {
		// Execute your transactional operations here.
		// Example: _, err := tx.Exec("UPDATE users SET last_login = ? WHERE id = ?",
		// time.Now(), 1)
		return nil
	}, dbkit.WithRetryPolicy(retryPolicy)); err != nil {
		log.Fatal(err)
	}
}
```

### `dbrutil` Usage Example

The following basic example demonstrates how to use `dbrutil` to open a database connection with instrumentation,
and execute queries with an automatic slow query logging and Prometheus metrics collection within transaction.

```go
package main

import (
	"context"
	"database/sql"
	"fmt"
	stdlog "log"
	"os"
	"time"

	"github.com/acronis/go-appkit/log"
	"github.com/gocraft/dbr/v2"

	"github.com/acronis/go-dbkit"
	"github.com/acronis/go-dbkit/dbrutil"
)

func main() {
	logger, loggerClose := log.NewLogger(&log.Config{
		Output: log.OutputStderr,
		Level:  log.LevelInfo,
	})
	defer loggerClose()

	// Create a Prometheus metrics collector.
	promMetrics := dbkit.NewPrometheusMetrics()
	promMetrics.MustRegister()
	defer promMetrics.Unregister()

	// Open the database connection with instrumentation.
	// Instrumentation includes collecting metrics about SQL queries and logging slow queries.
	eventReceiver := dbrutil.NewCompositeReceiver([]dbr.EventReceiver{
		dbrutil.NewQueryMetricsEventReceiver(promMetrics, queryAnnotationPrefix),
		dbrutil.NewSlowQueryLogEventReceiver(logger, 100*time.Millisecond, queryAnnotationPrefix),
	})
	conn, err := openDB(eventReceiver)
	if err != nil {
		stdlog.Fatal(err)
	}
	defer conn.Close()

	txRunner := dbrutil.NewTxRunner(conn, &sql.TxOptions{Isolation: sql.LevelReadCommitted}, nil)

	// Execute function in a transaction.
	// The transaction will be automatically committed if the function returns nil, otherwise it
	// will be rolled back.
	if dbErr := txRunner.DoInTx(context.Background(), func(tx dbr.SessionRunner) error {
		var result int
		return tx.Select("SLEEP(1)").
			// Annotate the query for Prometheus metrics and slow query log.
			Comment(annotateQuery("long_operation")).
			LoadOne(&result)
	}); dbErr != nil {
		stdlog.Fatal(dbErr)
	}

	// The following log message will be printed:
	// {"level":"warn","time":"2025-02-14T16:29:55.429257+02:00","msg":"slow SQL query",
	// "pid":14030, "annotation":"query:long_operation","duration_ms":1007}

	// Prometheus metrics will be collected:
	// db_query_duration_seconds_bucket{query="query:long_operation",le="2.5"} 1
	// db_query_duration_seconds_sum{query="query:long_operation"} 1.004573875
	// db_query_duration_seconds_count{query="query:long_operation"} 1
}

const queryAnnotationPrefix = "query:"

func annotateQuery(queryName string) string {
	return queryAnnotationPrefix + queryName
}

func openDB(eventReceiver dbr.EventReceiver) (*dbr.Connection, error) {
	cfg := &dbkit.Config{
		Dialect: dbkit.DialectMySQL,
		MySQL: dbkit.MySQLConfig{
			Host:     os.Getenv("MYSQL_HOST"),
			Port:     3306,
			User:     os.Getenv("MYSQL_USER"),
			Password: os.Getenv("MYSQL_PASSWORD"),
			Database: os.Getenv("MYSQL_DATABASE"),
		},
	}

	// Open database with instrumentation based on the provided event receiver
	// (see github.com/gocraft/dbr doc for details).
	// Opening includes configuring the max open/idle connections and their lifetime and
	// pinging the database.
	conn, err := dbrutil.Open(cfg, true, eventReceiver)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	return conn, nil
}
```

More examples and detailed usage instructions can be found in the `dbrutil` package [README](./dbrutil/README.md).

### `distrlock` Usage Example

The following basic example demonstrates how to use `distrlock` to ensure exclusive execution of a critical section of code.

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

More examples and detailed usage instructions can be found in the `distrlock` package [README](./distrlock/README.md).

## License

Copyright © 2024-2025 Acronis International GmbH.

Licensed under [MIT License](./LICENSE).

[GoDoc]: https://pkg.go.dev/github.com/acronis/go-dbkit
[GoDoc Widget]: https://godoc.org/github.com/acronis/go-dbkit?status.svg
