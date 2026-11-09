/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package testing

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mariadb"
	"github.com/testcontainers/testcontainers-go/modules/mssql"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/acronis/go-dbkit"
)

const (
	defaultTestConnMaxLifetime = 3 * time.Minute
	defaultTestMaxOpenConns    = 16
	defaultTestMaxIdleConns    = 16
)

// Container images of test databases. Tags are pinned to exact versions.
const (
	PostgresImage = "postgres:16.15-alpine"
	MySQLImage    = "mysql:8.4.13"
	MariaDBImage  = "mariadb:11.4.13"
	MSSQLImage    = "mcr.microsoft.com/mssql/server:2022-CU27-ubuntu-22.04"
)

const (
	testDBUser     = "root"
	testDBPassword = "password"
	testDBName     = "testdb"

	// mssqlPassword satisfies the SQL Server password policy. Test-only credential.
	mssqlPassword = "Test-Passw0rd!"

	startupTimeout   = 5 * time.Minute // Includes the image pull.
	readinessTimeout = time.Minute
	cleanupTimeout   = time.Minute
	logsTailBytes    = 4096
)

// DialectMariaDB selects a MariaDB server for RunTestDB. It uses the dbkit "mysql" dialect and driver.
const DialectMariaDB = "mariadb"

// MustRunAndOpenTestDB creates a container with a test database and returns a connection to it.
func MustRunAndOpenTestDB(ctx context.Context, dialect string) (db *sql.DB, stop func(ctx context.Context) error) {
	var err error
	if db, stop, err = RunAndOpenTestDB(ctx, dialect); err != nil {
		panic(fmt.Errorf("run and open test db: %w", err))
	}
	return
}

// testDB describes a started database container.
type testDB struct {
	container  testcontainers.Container
	driverName string
	dsn        string
	// setup prepares the database after the server becomes reachable and returns the DSN to use.
	setup func(ctx context.Context, db *sql.DB) (dsn string, err error)
}

// TestDB is a running test database.
type TestDB struct {
	DriverName string
	DSN        string
	// Stop terminates the container.
	Stop func(ctx context.Context) error
}

// RunAndOpenTestDB starts a container with a test database for the dialect and returns a connection to it.
// See RunTestDB for supported dialects and failure handling.
func RunAndOpenTestDB(ctx context.Context, dialect string) (db *sql.DB, stop func(ctx context.Context) error, err error) {
	tdb, err := RunTestDB(ctx, dialect)
	if err != nil {
		return nil, nil, err
	}
	readyCtx, readyCancel := context.WithTimeout(ctx, readinessTimeout)
	defer readyCancel()
	if db, err = openAndPing(readyCtx, tdb.DriverName, tdb.DSN); err != nil {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer cleanupCancel()
		return nil, nil, errors.Join(err, tdb.Stop(cleanupCtx))
	}
	return db, func(ctx context.Context) error {
		var errs []error
		if closeErr := db.Close(); closeErr != nil {
			errs = append(errs, fmt.Errorf("close db: %w", closeErr))
		}
		if stopErr := tdb.Stop(ctx); stopErr != nil {
			errs = append(errs, fmt.Errorf("stop db container: %w", stopErr))
		}
		return errors.Join(errs...)
	}, nil
}

// RunTestDB starts a container with a test database for the dialect and waits until PingContext succeeds.
// Supported dialects: "postgres" (lib/pq driver), "pgx", "mysql" (MySQL server, InnoDB, parseTime=true),
// "mariadb" (MariaDB server, "mysql" driver, InnoDB, parseTime=true)
// and "mssql" (SQL Server, "sqlserver" driver). The caller registers the database driver.
// Startup and readiness are bounded by timeouts derived from ctx. On failure the container is terminated
// and the error contains the image and the container log tail.
func RunTestDB(ctx context.Context, dialect string) (res *TestDB, err error) {
	var tdb testDB
	var image string

	startCtx, startCancel := context.WithTimeout(ctx, startupTimeout)
	defer startCancel()

	var start func(ctx context.Context) (testDB, error)
	switch dialect {
	case string(dbkit.DialectPgx), string(dbkit.DialectPostgres):
		image = PostgresImage
		start = func(ctx context.Context) (testDB, error) { return startPostgresContainer(ctx, dialect) }
	case string(dbkit.DialectMySQL):
		image, start = MySQLImage, startMySQLContainer
	case DialectMariaDB:
		image, start = MariaDBImage, startMariaDBContainer
	case string(dbkit.DialectMSSQL):
		image, start = MSSQLImage, startMSSQLContainer
	default:
		return nil, fmt.Errorf("unknown sql dialect %s", dialect)
	}
	tdb, err = startContainer(startCtx, start)

	terminate := func(ctx context.Context) error {
		return testcontainers.TerminateContainer(tdb.container, testcontainers.StopContext(ctx))
	}
	defer func() {
		if err == nil {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer cleanupCancel()
		err = fmt.Errorf("%s test db (image %s): %w%s", dialect, image, err, containerLogs(cleanupCtx, tdb.container))
		if termErr := terminate(cleanupCtx); termErr != nil {
			err = errors.Join(err, fmt.Errorf("terminate container: %w", termErr))
		}
	}()
	if err != nil {
		return nil, fmt.Errorf("start container: %w", err)
	}

	readyCtx, readyCancel := context.WithTimeout(ctx, readinessTimeout)
	defer readyCancel()

	db, err := openAndPing(readyCtx, tdb.driverName, tdb.dsn)
	if err != nil {
		return nil, err
	}
	dsn := tdb.dsn
	if tdb.setup != nil {
		dsn, err = tdb.setup(readyCtx, db)
		if err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("set up database: %w", err)
		}
	}
	if err = db.Close(); err != nil {
		return nil, fmt.Errorf("close db: %w", err)
	}
	return &TestDB{DriverName: tdb.driverName, DSN: dsn, Stop: terminate}, nil
}

// startContainer runs start and returns a Testcontainers panic as an error, so the caller fails with the backend and
// image in the message. Testcontainers panics, for example, when no Docker host is found.
func startContainer(ctx context.Context, start func(ctx context.Context) (testDB, error)) (tdb testDB, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("testcontainers panic: %v", p)
		}
	}()
	return start(ctx)
}

// openAndPing opens a connection pool and retries PingContext until it succeeds or ctx is done.
func openAndPing(ctx context.Context, driverName, dsn string) (*sql.DB, error) {
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetConnMaxLifetime(defaultTestConnMaxLifetime)
	db.SetMaxOpenConns(defaultTestMaxOpenConns)
	db.SetMaxIdleConns(defaultTestMaxIdleConns)

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		pingErr := db.PingContext(ctx)
		if pingErr == nil {
			return db, nil
		}
		select {
		case <-ctx.Done():
			_ = db.Close()
			return nil, fmt.Errorf("ping db: %w", errors.Join(pingErr, ctx.Err()))
		case <-ticker.C:
		}
	}
}

// containerLogs returns the tail of the container log formatted for appending to an error message.
func containerLogs(ctx context.Context, ctr testcontainers.Container) string {
	if ctr == nil {
		return ""
	}
	rc, err := ctr.Logs(ctx)
	if err != nil {
		return fmt.Sprintf("\ncontainer logs are unavailable: %v", err)
	}
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(rc)
	if err != nil && len(data) == 0 {
		return fmt.Sprintf("\ncontainer logs are unavailable: %v", err)
	}
	if len(data) > logsTailBytes {
		data = data[len(data)-logsTailBytes:]
	}
	return "\ncontainer logs (tail):\n" + string(data)
}

func startPostgresContainer(ctx context.Context, driverName string) (testDB, error) {
	ctr, err := postgres.Run(ctx,
		PostgresImage,
		postgres.WithDatabase(testDBName),
		postgres.WithUsername(testDBUser),
		postgres.WithPassword(testDBPassword),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(time.Minute),
			// Ensures the port mapping is published before the connection string is read.
			wait.ForListeningPort("5432/tcp")),
	)
	tdb := testDB{driverName: driverName}
	if ctr != nil {
		tdb.container = ctr
	}
	if err != nil {
		return tdb, err
	}
	tdb.dsn, err = ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return tdb, fmt.Errorf("get connection string: %w", err)
	}
	return tdb, nil
}

func startMySQLContainer(ctx context.Context) (testDB, error) {
	ctr, err := mysql.Run(ctx,
		MySQLImage,
		mysql.WithDatabase(testDBName),
		mysql.WithUsername(testDBUser),
		mysql.WithPassword(testDBPassword),
		testcontainers.WithCmdArgs("--default-storage-engine=InnoDB"),
		testcontainers.WithAdditionalWaitStrategy(wait.ForListeningPort("3306/tcp")),
	)
	tdb := testDB{driverName: string(dbkit.DialectMySQL)}
	if ctr != nil {
		tdb.container = ctr
	}
	if err != nil {
		return tdb, err
	}
	tdb.dsn, err = ctr.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		return tdb, fmt.Errorf("get connection string: %w", err)
	}
	return tdb, nil
}

func startMariaDBContainer(ctx context.Context) (testDB, error) {
	ctr, err := mariadb.Run(ctx,
		MariaDBImage,
		mariadb.WithDatabase(testDBName),
		mariadb.WithUsername(testDBUser),
		mariadb.WithPassword(testDBPassword),
		testcontainers.WithCmdArgs("--default-storage-engine=InnoDB"),
		testcontainers.WithAdditionalWaitStrategy(wait.ForListeningPort("3306/tcp")),
	)
	tdb := testDB{driverName: string(dbkit.DialectMySQL)}
	if ctr != nil {
		tdb.container = ctr
	}
	if err != nil {
		return tdb, err
	}
	tdb.dsn, err = ctr.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		return tdb, fmt.Errorf("get connection string: %w", err)
	}
	return tdb, nil
}

func startMSSQLContainer(ctx context.Context) (testDB, error) {
	ctr, err := mssql.Run(ctx,
		MSSQLImage,
		mssql.WithAcceptEULA(),
		mssql.WithPassword(mssqlPassword),
		// Microsoft publishes SQL Server Linux images for x86-64 only.
		testcontainers.WithImagePlatform("linux/amd64"),
	)
	tdb := testDB{driverName: "sqlserver"}
	if ctr != nil {
		tdb.container = ctr
	}
	if err != nil {
		return tdb, err
	}
	tdb.dsn, err = ctr.ConnectionString(ctx)
	if err != nil {
		return tdb, fmt.Errorf("get connection string: %w", err)
	}
	baseDSN := tdb.dsn
	tdb.setup = func(ctx context.Context, db *sql.DB) (string, error) {
		if _, err := db.ExecContext(ctx, "CREATE DATABASE "+testDBName); err != nil {
			return "", fmt.Errorf("create database: %w", err)
		}
		u, err := url.Parse(baseDSN)
		if err != nil {
			return "", fmt.Errorf("parse dsn: %w", err)
		}
		q := u.Query()
		q.Set("database", testDBName)
		u.RawQuery = q.Encode()
		return u.String(), nil
	}
	return tdb, nil
}
