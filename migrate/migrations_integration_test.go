/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package migrate_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	gotesting "testing"
	"time"

	"github.com/acronis/go-appkit/log/logtest"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/require"

	"github.com/acronis/go-dbkit"
	"github.com/acronis/go-dbkit/internal/testing"
	"github.com/acronis/go-dbkit/migrate"
	_ "github.com/acronis/go-dbkit/postgres"
)

// mssqlDSNEnvVar names the environment variable with a DSN of a disposable MSSQL database.
// MSSQL checks are skipped when it is not set.
const mssqlDSNEnvVar = "DBKIT_TEST_MSSQL_DSN"

func TestMigrationsManager_Postgres(t *gotesting.T) {
	runMigrationsSmokeTests(t, dbkit.DialectPostgres)
}

func TestMigrationsManager_Pgx(t *gotesting.T) {
	runMigrationsSmokeTests(t, dbkit.DialectPgx)
}

func TestMigrationsManager_MySQL(t *gotesting.T) {
	runMigrationsSmokeTests(t, dbkit.DialectMySQL)
}

func TestMigrationsManager_MSSQL(t *gotesting.T) {
	dsn := os.Getenv(mssqlDSNEnvVar)
	if dsn == "" {
		t.Skipf("%s is not set", mssqlDSNEnvVar)
	}
	db, err := sql.Open("sqlserver", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.NoError(t, db.Ping())
	runMigrationsSmokeChecks(t, db, dbkit.DialectMSSQL)
}

func runMigrationsSmokeTests(t *gotesting.T, dialect dbkit.Dialect) {
	db, stop, err := openTestDB(dialect)
	if err != nil {
		if os.Getenv("CI") == "" {
			t.Skipf("test database is unavailable: %v", err)
		}
		require.NoError(t, err)
	}
	t.Cleanup(func() { require.NoError(t, stop(context.Background())) })
	runMigrationsSmokeChecks(t, db, dialect)
}

// openTestDB starts a test database container. Testcontainers panics when Docker is unavailable, so the panic is turned into an error.
func openTestDB(dialect dbkit.Dialect) (db *sql.DB, stop func(context.Context) error, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("start test database: %v", r)
		}
	}()
	return testing.RunAndOpenTestDB(context.Background(), string(dialect))
}

func smokeMigrations() []migrate.Migration {
	return []migrate.Migration{
		migrate.NewCustomMigration("0001_create_users",
			[]string{"CREATE TABLE smoke_users (id INT NOT NULL PRIMARY KEY, name VARCHAR(100))"},
			[]string{"DROP TABLE smoke_users"}, nil, nil),
		migrate.NewCustomMigration("0002_seed_users",
			[]string{"INSERT INTO smoke_users (id, name) VALUES (1, 'alice')", "INSERT INTO smoke_users (id, name) VALUES (2, 'bob');"},
			[]string{"DELETE FROM smoke_users"}, nil, nil),
	}
}

func countSmokeUsers(t *gotesting.T, db *sql.DB) (int, error) {
	t.Helper()
	var n int
	err := db.QueryRow("SELECT COUNT(*) FROM smoke_users").Scan(&n)
	return n, err
}

// runMigrationsSmokeChecks checks metadata table creation, up/down and status for the default and a custom table name.
func runMigrationsSmokeChecks(t *gotesting.T, db *sql.DB, dialect dbkit.Dialect) {
	for _, tableName := range []string{"", "smoke_custom_migrations"} {
		t.Run(fmt.Sprintf("table=%q", tableName), func(t *gotesting.T) {
			mm, err := migrate.NewMigrationsManagerWithOpts(db, dialect, logtest.NewLogger(),
				migrate.MigrationsManagerOpts{TableName: tableName})
			require.NoError(t, err)
			migs := smokeMigrations()

			status, err := mm.Status()
			require.NoError(t, err)
			require.Empty(t, status.AppliedMigrations)

			require.NoError(t, mm.RunLimit(migs, migrate.MigrationsDirectionUp, 1))
			require.NoError(t, mm.Run(migs, migrate.MigrationsDirectionUp))
			n, err := countSmokeUsers(t, db)
			require.NoError(t, err)
			require.Equal(t, 2, n)

			status, err = mm.Status()
			require.NoError(t, err)
			require.Len(t, status.AppliedMigrations, 2)
			last, ok := status.LastAppliedMigration()
			require.True(t, ok)
			require.Equal(t, "0002_seed_users", last.ID)
			require.WithinDuration(t, time.Now(), last.AppliedAt, time.Minute)

			// Applying again is a no-op.
			require.NoError(t, mm.Run(migs, migrate.MigrationsDirectionUp))

			require.NoError(t, mm.Run(migs, migrate.MigrationsDirectionDown))
			_, err = countSmokeUsers(t, db)
			require.Error(t, err)
			status, err = mm.Status()
			require.NoError(t, err)
			require.Empty(t, status.AppliedMigrations)
		})
	}
}
