# Toolkit for working with SQL databases in Go

## Structure

### `/`
Package `db` provides helpers for working with different SQL databases (MySQL, PostgreSQL, SQLite and MSSQL).

### `/distrlock`
Package distrlock contains DML (distributed lock manager) implementation (now DMLs based on MySQL and PostgreSQL are supported).
Now only manager that uses SQL database (PostgreSQL and MySQL are currently supported) is available.
Other implementations (for example, based on Redis) will probably be implemented in the future.

### `/migrate`
Package migrate provides functionality for applying database migrations.

### `/mssql`
Package mssql provides helpers for working with MSSQL.
Should be imported explicitly.
To register mssql as retryable func use side effect import like so:

```go
import _ "github.com/acronis/go-dbkit/mssql"
```

### `/mysql`
Package mysql provides helpers for working with MySQL.
Should be imported explicitly.
To register mysql as retryable func use side effect import like so:

```go
import _ "github.com/acronis/go-dbkit/mysql"
```

### `/pgx`
Package pgx provides helpers for working with Postgres via `jackc/pgx` driver.
Should be imported explicitly.
To register postgres as retryable func use side effect import like so:

```go
import _ "github.com/acronis/go-dbkit/pgx"
```

### `/postgres`
Package postgres provides helpers for working with Postgres via `lib/pq` driver.
Should be imported explicitly.
To register postgres as retryable func use side effect import like so:

```go
import _ "github.com/acronis/go-dbkit/postgres"
```

### `/sqlite`
Package sqlite provides helpers for working with SQLite.
Should be imported explicitly.
To register sqlite as retryable func use side effect import like so:

```go
import _ "github.com/acronis/go-dbkit/sqlite"
```

### `/dbrutil`
Package dbrutil provides utilities and helpers for [dbr](https://github.com/gocraft/dbr) query builder.

### `/goquutil`
Package goquutil provides auxiliary routines for working with [goqu](https://github.com/doug-martin/goqu) query builder.

## Examples

### Open database connection using the `dbrutil` package

```go
func main() {
	// Create a new database configuration
	cfg := &db.Config{
		Driver:   db.DialectMySQL,
		Host:     "localhost",
		Port:     3306,
		Username: "your-username",
		Password: "your-password",
		Database: "your-database",
	}

	// Open a connection to the database
	conn, err := dbrutil.Open(cfg, true, nil)
	if err != nil {
		fmt.Println("Failed to open database connection:", err)
		return
	}
	defer conn.Close()

	// Create a new transaction runner
	runner := dbrutil.NewTxRunner(conn, &sql.TxOptions{}, nil)

	// Execute code inside a transaction
	err = runner.DoInTx(context.Background(), func(runner dbr.SessionRunner) error {
		// Perform database operations using the runner
		_, err := runner.InsertInto("users").
			Columns("name", "email").
			Values("Bob", "bob@example.com").
			Exec()
		if err != nil {
			return err
		}

		// Return nil to commit the transaction
		return nil
	})
	if err != nil {
		fmt.Println("Failed to execute transaction:", err)
		return
	}
}
```

### Usage of `distrlock` package

```go
// Create a new DBManager with the MySQL dialect
dbManager, err := distrlock.NewDBManager(db.DialectMySQL)
if err != nil {
    log.Fatal(err)
}

// Open a connection to the MySQL database
dbConn, err := sql.Open("mysql", "user:password@tcp(localhost:3306)/database")
if err != nil {
    log.Fatal(err)
}
defer dbConn.Close()

// Create a new lock
lock, err := dbManager.NewLock(context.Background(), dbConn, "my_lock")
if err != nil {
    log.Fatal(err)
}

// Acquire the lock
err = lock.Acquire(context.Background(), dbConn, 5*time.Second)
if err != nil {
    log.Fatal(err)
}

// Do some work while holding the lock
fmt.Println("Lock acquired, doing some work...")

// Release the lock
err = lock.Release(context.Background(), dbConn)
if err != nil {
    log.Fatal(err)
}

fmt.Println("Lock released")
```

## License

Copyright © 2024 Acronis International GmbH.

Licensed under [MIT License](./LICENSE).
