/*
Copyright © 2025 Acronis International GmbH.

Released under MIT license.
*/

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
