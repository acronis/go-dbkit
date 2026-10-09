# migrate

[![GoDoc Widget]][GoDoc]

`migrate` package provides functionality for applying database migrations in your Go applications. It uses a built-in engine on top of `database/sql` with no external migration library. Supported dialects: SQLite, PostgreSQL (`postgres`, `pgx`), MySQL/MariaDB, MSSQL.

It offers two primary approaches for defining your migrations:
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

## Engine behavior

- Applied migrations are stored in the `migrations` table (configurable with `MigrationsManagerOpts.TableName`) with columns `id` (primary key) and `applied_at` (nullable). Tables created by earlier versions are used as is; applied migrations are not replayed.
- Migrations are ordered by the numeric ID prefix, then lexicographically. IDs with a numeric prefix go before others.
- Each migration and its metadata update run in one transaction. A migration implementing `TxDisabler` runs outside a transaction; its metadata is recorded only after all statements succeed.
- Migrations absent from the history but ordered before the last applied one are applied first, in both directions, and are not counted in `limit`.
- A migration recorded in the database but missing from the passed list fails the run.
- Statements are executed one by one; a trailing line break, space, or `;` is trimmed.
- MySQL: `parseTime=true` is required in the DSN for `Status`.
- `UpFn` / `DownFn` are not supported.

### Raw migrations

A migration implementing `RawMigrator` replaces the default conversion. Returning `nil` falls back to `UpSQL`/`DownSQL`.

```go
type Migration0003 struct {
	*migrate.NullMigration
}

func (m *Migration0003) ID() string { return "0003_seed_users" }

func (m *Migration0003) RawMigration(self migrate.Migration) (*migrate.RawMigration, error) {
	return &migrate.RawMigration{
		ID:                     self.ID(),
		UpSQL:                  []string{"INSERT INTO users (name) VALUES ('admin')"},
		DownSQL:                []string{"DELETE FROM users WHERE name = 'admin'"},
		DisableTransactionUp:   false,
		DisableTransactionDown: false,
	}, nil
}
```

### Differences from the sql-migrate based implementation

- `RawMigrator.RawMigration` returns `*migrate.RawMigration` instead of `*sql-migrate.Migration`. Replace `migrate.ParseMigration` (`-- +migrate Up/Down` comments) with explicit `UpSQL`/`DownSQL`; `+migrate NoTransaction` maps to `DisableTransactionUp/Down`.
- The `parseTime` check on MySQL is no longer done on every run, only `Status` needs it.
- New metadata tables use `VARCHAR(255)` / `DATETIME` (`TIMESTAMP WITH TIME ZONE` on PostgreSQL) without explicit engine and charset on MySQL.
- A `NULL` `applied_at` is returned as zero time instead of failing.
- An unknown direction error reports the direction value.

## License

Copyright © 2025 Acronis International GmbH.

Licensed under [MIT License](./../LICENSE).

[GoDoc]: https://pkg.go.dev/github.com/acronis/go-dbkit/migrate
[GoDoc Widget]: https://godoc.org/github.com/acronis/go-dbkit/migrate?status.svg