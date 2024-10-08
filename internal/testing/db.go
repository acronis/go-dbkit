/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package testing

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mariadb"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	defaultTestConnMaxLifetime = 3 * time.Minute
	defaultTestMaxOpenConns    = 16
	defaultTestMaxIdleConns    = 16
)

// MustRunAndOpenTestDB creates a container with a test database and returns a connection to it.
func MustRunAndOpenTestDB(ctx context.Context, dialect string) (db *sql.DB, stop func(ctx context.Context) error) {
	var err error
	if db, stop, err = RunAndOpenTestDB(ctx, dialect); err != nil {
		panic(fmt.Errorf("run and open test db: %w", err))
	}
	return
}

func RunAndOpenTestDB(ctx context.Context, dialect string) (db *sql.DB, stop func(ctx context.Context) error, err error) {
	var dsn string
	var stopCt func(ctx context.Context) error
	switch dialect {
	case "pgx", "postgres":
		if dsn, stopCt, err = startPostgresContainer(ctx); err != nil {
			return nil, nil, fmt.Errorf("start postgres container: %w", err)
		}
	case "mysql":
		if dsn, stopCt, err = startMariaDBContainer(ctx); err != nil {
			return nil, nil, fmt.Errorf("start mariadb container: %w", err)
		}
	default:
		return nil, nil, fmt.Errorf("unknown sql dialect %s", dialect)
	}

	defer func() {
		if err != nil {
			_ = stopCt(ctx)
		}
	}()

	if db, err = sql.Open(dialect, dsn); err != nil {
		return nil, stopCt, fmt.Errorf("open db: %w", err)
	}
	defer func() {
		if err != nil {
			_ = db.Close()
		}
	}()

	db.SetConnMaxLifetime(defaultTestConnMaxLifetime)
	db.SetMaxOpenConns(defaultTestMaxOpenConns)
	db.SetMaxIdleConns(defaultTestMaxIdleConns)

	if err = db.Ping(); err != nil {
		return db, stopCt, fmt.Errorf("ping db: %w", err)
	}

	return db, func(ctx context.Context) error {
		var resErr error
		if closeDBErr := db.Close(); closeDBErr != nil {
			resErr = fmt.Errorf("close db: %w", closeDBErr)
		}
		if stopErr := stopCt(ctx); stopErr != nil {
			resErr = fmt.Errorf("stop db container: %w", stopErr)
		}
		return resErr
	}, nil
}

func startPostgresContainer(ctx context.Context) (dsn string, stop func(ctx context.Context) error, err error) {
	const (
		dbUser     = "root"
		dbPassword = "password"
		dbName     = "testdb"
	)
	postgresContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPassword),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(10*time.Second)),
	)
	if err != nil {
		return "", nil, fmt.Errorf("create container: %w", err)
	}
	defer func() {
		if err != nil {
			_ = postgresContainer.Terminate(ctx)
		}
	}()
	if dsn, err = postgresContainer.ConnectionString(ctx, "sslmode=disable"); err != nil {
		return "", nil, fmt.Errorf("get connection string: %w", err)
	}
	return dsn, postgresContainer.Terminate, nil
}

func startMariaDBContainer(ctx context.Context) (dsn string, stop func(ctx context.Context) error, err error) {
	const (
		dbUser     = "root"
		dbPassword = "password"
		dbName     = "testdb"
	)
	mariaDBContainer, err := mariadb.Run(ctx,
		"mariadb:11.0.3",
		mariadb.WithDatabase(dbName),
		mariadb.WithUsername(dbUser),
		mariadb.WithPassword(dbPassword),
	)
	if err != nil {
		return "", nil, fmt.Errorf("create container: %w", err)
	}
	defer func() {
		if err != nil {
			_ = mariaDBContainer.Terminate(ctx)
		}
	}()
	if dsn, err = mariaDBContainer.ConnectionString(ctx, "parseTime=true"); err != nil {
		return "", nil, fmt.Errorf("get connection string: %w", err)
	}
	return dsn, mariaDBContainer.Terminate, nil
}
