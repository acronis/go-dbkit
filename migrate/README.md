# migrate

[![GoDoc Widget]][GoDoc]

The `migrate` package provides functionality for applying database migrations in your Go applications. It leverages [github.com/rubenv/sql-migrate](https://github.com/rubenv/sql-migrate) under the hood, ensuring a reliable and consistent approach to managing database schema changes.

## Overview

The `migrate` package offers two primary approaches for defining your migrations:
- **Embedded SQL Migrations**: Store your migrations as plain SQL files (with separate `.up.sql` and `.down.sql` files) and embed them into your Go binary using Go's built-in embed package. This approach is straightforward and keeps your SQL scripts separate from your application code.
- **Programmatic SQL Migrations**: Define your migrations directly in Go code. This method is more suitable when you require additional customization or more control over your migrations. It lets you write migrations as Go functions, while still leveraging SQL commands.

## Usage

The examples below show how to define migrations for creating a "users" table and a "notes" table.

### Running Embedded SQL Migrations

You can embed your SQL migration files into your binary with Go's embed package.
The following example (from the [examples/embedded-sql-migrations](./examples/embedded-sql-migrations) directory) demonstrates how to load and execute embedded migrations:

```go
package main

import (
	"database/sql"
	"embed"
	"flag"
	"fmt"
	stdlog "log"
	"os"

	"github.com/acronis/go-appkit/log"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/lib/pq"

	"github.com/acronis/go-dbkit"
	"github.com/acronis/go-dbkit/migrate"
)

//go:embed mysql/*.sql
//go:embed postgres/*.sql
var migrationFS embed.FS

func main() {
	if err := runMigrations(); err != nil {
		stdlog.Fatal(err)
	}
}

func runMigrations() error {
	var migrateDown bool
	flag.BoolVar(&migrateDown, "down", false, "migrate down")
	var driverName string
	flag.StringVar(&driverName, "driver", "", "driver name, supported values: mysql, postgres, pgx")
	flag.Parse()

	migrationDirection := migrate.MigrationsDirectionUp
	if migrateDown {
		migrationDirection = migrate.MigrationsDirectionDown
	}

	dialect, migrationDirName, err := parseDialectFromDriver(driverName)
	if err != nil {
		return fmt.Errorf("parse dialect: %w", err)
	}

	dbConn, err := sql.Open(driverName, os.Getenv("DB_DSN"))
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}

	logger, loggerClose := log.NewLogger(&log.Config{Output: log.OutputStderr, Level: log.LevelInfo})
	defer loggerClose()

	migrationManager, err := migrate.NewMigrationsManager(dbConn, dialect, logger)
	if err != nil {
		return err
	}
	migrations, err := migrate.LoadAllEmbedFSMigrations(migrationFS, migrationDirName)
	if err != nil {
		return fmt.Errorf("make embed fs migrations: %w", err)
	}
	return migrationManager.Run(migrations, migrationDirection)
}

func parseDialectFromDriver(driverName string) (dialect dbkit.Dialect, migrationDirName string, err error) {
	switch driverName {
	case "mysql":
		return dbkit.DialectMySQL, "mysql", nil
	case "postgres":
		return dbkit.DialectPostgres, "postgres", nil
	case "pgx":
		return dbkit.DialectPgx, "postgres", nil
	default:
		return "", "", fmt.Errorf("unknown driver name: %s", driverName)
	}
}
```

### Defining SQL Migrations in Go Files

For greater control or when you need to include custom logic, you can define your migrations directly in Go.
This approach is demonstrated in the following example from the [examples/go-sql-migrations](examples/go-sql-migrations) directory.
The example includes two migration files that define the creation and deletion of the "users" and "notes" tables.

**Main Application**

```go
package main

import (
	"database/sql"
	"flag"
	"fmt"
	stdlog "log"
	"os"

	"github.com/acronis/go-appkit/log"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/lib/pq"

	"github.com/acronis/go-dbkit"
	"github.com/acronis/go-dbkit/migrate"
)

func main() {
	if err := runMigrations(); err != nil {
		stdlog.Fatal(err)
	}
}

func runMigrations() error {
	var migrateDown bool
	flag.BoolVar(&migrateDown, "down", false, "migrate down")
	var driverName string
	flag.StringVar(&driverName, "driver", "", "driver name, supported values: mysql, postgres, pgx")
	flag.Parse()

	migrationDirection := migrate.MigrationsDirectionUp
	if migrateDown {
		migrationDirection = migrate.MigrationsDirectionDown
	}

	dialect, err := parseDialectFromDriver(driverName)
	if err != nil {
		return fmt.Errorf("parse dialect: %w", err)
	}

	dbConn, err := sql.Open(driverName, os.Getenv("DB_DSN"))
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}

	logger, loggerClose := log.NewLogger(&log.Config{Output: log.OutputStderr, Level: log.LevelInfo})
	defer loggerClose()

	migrationManager, err := migrate.NewMigrationsManager(dbConn, dialect, logger)
	if err != nil {
		return err
	}
	return migrationManager.Run([]migrate.Migration{
		NewMigration0001CreateUsersTable(dialect),
		NewMigration0002CreateNotesTable(dialect),
	}, migrationDirection)
}

func parseDialectFromDriver(driverName string) (dbkit.Dialect, error) {
	switch driverName {
	case "mysql":
		return dbkit.DialectMySQL, nil
	case "postgres":
		return dbkit.DialectPostgres, nil
	case "pgx":
		return dbkit.DialectPgx, nil
	default:
		return "", fmt.Errorf("unknown driver name: %s", driverName)
	}
}
```

**Migration for Creating the Users Table**

```go
package main

import (
	"github.com/acronis/go-dbkit"
	"github.com/acronis/go-dbkit/migrate"
)

const migration0001CreateUsersTableUpMySQL = `
CREATE TABLE users (
    id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL
);
`

const migration0001CreateUsersTableUpPostgres = `
CREATE TABLE users (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);
`

const migration0001CreateUsersTableDown = `
DROP TABLE users;
`

type Migration0001CreateUsersTable struct {
	*migrate.NullMigration
}

func NewMigration0001CreateUsersTable(dialect dbkit.Dialect) *Migration0001CreateUsersTable {
	return &Migration0001CreateUsersTable{&migrate.NullMigration{Dialect: dialect}}
}

func (m *Migration0001CreateUsersTable) ID() string {
	return "0001_create_users_table"
}

func (m *Migration0001CreateUsersTable) UpSQL() []string {
	switch m.Dialect {
	case dbkit.DialectMySQL:
		return []string{migration0001CreateUsersTableUpMySQL}
	case dbkit.DialectPgx, dbkit.DialectPostgres:
		return []string{migration0001CreateUsersTableUpPostgres}
	}
	return nil
}

func (m *Migration0001CreateUsersTable) DownSQL() []string {
	switch m.Dialect {
	case dbkit.DialectMySQL, dbkit.DialectPgx, dbkit.DialectPostgres:
		return []string{migration0001CreateUsersTableDown}
	}
	return nil
}
```

**Migration for Creating the Notes Table**

```go
package main

import (
	"github.com/acronis/go-dbkit"
	"github.com/acronis/go-dbkit/migrate"
)

const migration0002CreateNotesTableUpMySQL = `
CREATE TABLE notes (
    id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    content TEXT,
    user_id BIGINT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
`

const migration0002CreateNotesTableUpPostgres = `
CREATE TABLE notes (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    content TEXT,
    user_id BIGINT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
`

const migration0002CreateNotesTableDown = `
DROP TABLE notes;
`

func NewMigration0002CreateNotesTable(dialect dbkit.Dialect) *migrate.CustomMigration {
	var upSQL []string
	var downSQL []string
	switch dialect {
	case dbkit.DialectMySQL:
		upSQL = []string{migration0002CreateNotesTableUpMySQL}
		downSQL = []string{migration0002CreateNotesTableDown}
	case dbkit.DialectPgx, dbkit.DialectPostgres:
		upSQL = []string{migration0002CreateNotesTableUpPostgres}
		downSQL = []string{migration0002CreateNotesTableDown}
	}
	return migrate.NewCustomMigration("0002_create_notes_table", upSQL, downSQL, nil, nil)
}
```

## License

Copyright © 2025 Acronis International GmbH.

Licensed under [MIT License](./../LICENSE).

[GoDoc]: https://pkg.go.dev/github.com/acronis/go-dbkit/migrate
[GoDoc Widget]: https://godoc.org/github.com/acronis/go-dbkit/migrate?status.svg