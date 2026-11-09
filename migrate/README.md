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
- A new metadata table has the schema created by earlier versions:

  | Dialect | `id` | `applied_at` | Table options |
  |---|---|---|---|
  | SQLite | `VARCHAR(255)` | `DATETIME` | |
  | PostgreSQL (`postgres`, `pgx`) | `TEXT` | `TIMESTAMP WITH TIME ZONE` | |
  | MySQL, MariaDB | `VARCHAR(255)` | `DATETIME` | `ENGINE=InnoDB DEFAULT CHARSET=utf8mb3` |
  | MSSQL | `VARCHAR(255)` | `DATETIME` | |

- Before reading the history, `Run`, `RunLimit` and `Status` check the existing metadata table in the database catalogs, on every call. They return an error naming the table, the dialect and the reasons, without running migration SQL or changing the table, when:
  - `id` or `applied_at` is missing or generated/computed (or an identity column that rejects explicit values);
  - `id` is not a character string type, is nullable, or has no primary key or unique index on it alone (composite, partial/filtered, expression, prefix-length and disabled indexes do not count);
  - `applied_at` is not a date and time type (date-only types are rejected);
  - another column is `NOT NULL` without a default and is not generated, identity or auto-increment (`DEFAULT NULL` is not a default).

  Accepted string types: SQLite `TEXT`/`VARCHAR`/`CHAR`/`CLOB` family; PostgreSQL `text`, `varchar`; MySQL/MariaDB `varchar`, `char`, `text` family; MSSQL `varchar`, `nvarchar`, `char`, `nchar`. Accepted date and time types: SQLite `DATETIME`, `TIMESTAMP`; PostgreSQL `timestamp` with or without time zone; MySQL/MariaDB `datetime`, `timestamp`; MSSQL `datetime`, `datetime2`, `smalldatetime`, `datetimeoffset`. Column order, extra optional columns, constraint names, engine and charset are not checked. Use a separate history table (`MigrationsManagerOpts.TableName`) when the existing table is incompatible. The check does not detect triggers, `CHECK` or foreign key constraints, permission changes or concurrent DDL.
- Migrations are ordered by the numeric ID prefix, then lexicographically. IDs with a numeric prefix go before others.
- Each migration and its metadata update run in one transaction. A migration implementing `TxDisabler` runs outside a transaction; its metadata is recorded only after all statements succeed.
- Migrations absent from the history but ordered before the last applied one ("gaps") run their up SQL first, in both directions, and are not counted in `limit`. Inherited from sql-migrate v1.0.0: on `down` a gap is not recorded in the history, and the down plan walks all migrations ordered before the last applied one, including gaps, so a later `down` that reaches a gap runs its up SQL again as catch-up and then its down SQL.
- A migration recorded in the database but missing from the passed list fails the run.
- Statements are executed one by one; a trailing line break, space, or `;` is trimmed.
- `applied_at` is written in UTC.
- MySQL: `parseTime=true` is required in the DSN for `Status`.
- `UpFn` / `DownFn` are not supported.

### Raw migrations

A migration implementing `RawMigrator` replaces the default conversion. Returning `nil` falls back to `UpSQL`/`DownSQL`. A `RawMigration` method with another signature (e.g. the previous one returning `*migrate.Migration` of sql-migrate) fails the run instead of being ignored.

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
- An existing metadata table is checked before every run; an incompatible table (see above) fails the run before migration SQL. sql-migrate failed such tables only when reading them (`SELECT *` into its record type) or when writing the history after the migration SQL.
- A `NULL` `applied_at` is returned as zero time instead of failing.
- An unknown direction error reports the direction value.
- `applied_at` is written in UTC. sql-migrate wrote the local time; on MSSQL (`DATETIME`, no time zone) it was read back as UTC, shifted by the host offset. Existing rows are not changed.

## Transactions

| Operation inside a migration transaction, then a failing statement | SQLite, PostgreSQL (`postgres`, `pgx`), MSSQL | MySQL, MariaDB (InnoDB) |
|---|---|---|
| DML | Rolled back | Rolled back |
| `CREATE TABLE` | Rolled back | Remains: DDL commits implicitly |
| `DROP TABLE` | Rolled back | Remains dropped |
| History row | Failed `up`: not inserted. Failed `down`: kept | Same |

With the transaction disabled (`TxDisabler`, `DisableTransactionUp/Down`), statements executed before the failure remain; the history changes only after all statements succeed.

## Testing

`TestMigrationsManagerIntegration` runs the same scenarios against SQLite, MySQL 8.4, MariaDB 11.4, PostgreSQL 16 via `postgres` and `pgx`, and SQL Server 2022, each with the default and a custom metadata table: lifecycle, failed up/down DML and DDL, transaction opt-out and raw transaction flags, metadata write failures, upgrade of databases written by the sql-migrate based implementation, history edge cases (NULL `applied_at`, unknown IDs, catch-up), metadata table schema, and compatible/incompatible existing tables.

```sh
# Full suite. Requires Docker; server databases run in Testcontainers.
go test -race -count=1 -timeout=15m ./migrate -run '^TestMigrationsManagerIntegration$'

# SQLite only. Starts no containers.
go test -race -count=1 ./migrate -run '^TestMigrationsManagerIntegration/SQLite$'
```

- Server backends are required: missing Docker or a failed container start fails the test; nothing is skipped.
- Image tags are pinned in `internal/testing/db.go`.
- SQL Server images are x86-64 only (`linux/amd64`). On ARM hosts they run under emulation, which Microsoft does not support; CI runs on Linux x86-64.
- Upgrade fixtures in [`testdata/upgrade`](testdata/upgrade/README.md) are databases written by the previous implementation (go-dbkit `d3d5fd6`, sql-migrate v1.0.0): a SQLite file and PostgreSQL, MySQL, MariaDB and MSSQL exports. Tests restore them and check their SHA-256; regeneration steps are in that README.

## License

Copyright © 2025 Acronis International GmbH.

Licensed under [MIT License](./../LICENSE).

[GoDoc]: https://pkg.go.dev/github.com/acronis/go-dbkit/migrate
[GoDoc Widget]: https://godoc.org/github.com/acronis/go-dbkit/migrate?status.svg