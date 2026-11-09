/*
Copyright © 2026 Acronis International GmbH.

Released under MIT license.
*/

package migrate_test

import (
	"fmt"
	gotesting "testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/acronis/go-dbkit"
	"github.com/acronis/go-dbkit/migrate"
)

// legacyCreateTable returns the metadata table DDL executed by previous versions of the package, which used
// sql-migrate v1.0.0 with gorp v1.7.2. Statements are copied verbatim with the table name substituted.
// They are not derived from the current implementation in dialect.go.
//
// Column types: id is varchar(255) (text on PostgreSQL); applied_at is datetime (timestamp with time zone on PostgreSQL).
// MySQL adds engine=InnoDB charset=UTF8. MSSQL checks information_schema and does not quote the table name.
//
// Reproduction, in a temporary module outside this repository: for each sql-migrate MigrationDialects entry, create
// gorp.DbMap{Dialect: d} over a database/sql driver that records executed statements, call
// AddTableWithNameAndSchema(MigrationRecord{}, "", table).SetKeys(false, "Id") and CreateTablesIfNotExists,
// and print the recorded statement.
func legacyCreateTable(dialect dbkit.Dialect, table string) string {
	switch dialect {
	case dbkit.DialectSQLite:
		return fmt.Sprintf(`create table if not exists "%s" ("id" varchar(255) not null primary key, "applied_at" datetime) ;`, table)
	case dbkit.DialectPostgres, dbkit.DialectPgx: // Both drivers share the PostgreSQL fixture.
		return fmt.Sprintf(`create table if not exists "%s" ("id" text not null primary key, "applied_at" timestamp with time zone) ;`,
			table)
	case dbkit.DialectMySQL:
		return fmt.Sprintf("create table if not exists `%s` (`id` varchar(255) not null primary key, `applied_at` datetime)"+
			"  engine=InnoDB charset=UTF8;", table)
	case dbkit.DialectMSSQL:
		return fmt.Sprintf(`if not exists (select * from information_schema.tables where table_name = '%[1]s') `+
			`create table %[1]s ("id" varchar(255) not null primary key, "applied_at" datetime) ;`, table)
	default:
		panic("unknown dialect " + dialect)
	}
}

// legacyAppliedAtType returns the applied_at column type of legacyCreateTable.
func legacyAppliedAtType(dialect dbkit.Dialect) string {
	switch dialect {
	case dbkit.DialectPostgres, dbkit.DialectPgx:
		return "timestamp with time zone"
	default:
		return "datetime"
	}
}

func legacyHistoryScenarios() []integrationScenario {
	return []integrationScenario{
		{"LegacyHistory", testLegacyHistory},
		{"LegacyNullAppliedAt", testLegacyNullAppliedAt},
		{"LegacyUnknownAppliedID", testLegacyUnknownAppliedID},
		{"LegacyCatchUpGap", testLegacyCatchUpGap},
	}
}

// createLegacyHistory creates the metadata table as previous versions did and inserts records.
// A zero time is stored as NULL.
func (e *integrationEnv) createLegacyHistory(records ...legacyRecord) {
	e.t.Helper()
	e.exec(legacyCreateTable(e.b.dialect, e.metaTable))
	for _, rec := range records {
		var appliedAt any
		if !rec.appliedAt.IsZero() {
			appliedAt = rec.appliedAt
		}
		e.exec(fmt.Sprintf("INSERT INTO {meta} (id, applied_at) VALUES (%s, %s)", e.b.placeholder(1), e.b.placeholder(2)),
			rec.id, appliedAt)
	}
}

type legacyRecord struct {
	id        string
	appliedAt time.Time
}

// testLegacyHistory reads and extends history written by the previous implementation without replaying it.
func testLegacyHistory(t *gotesting.T, e *integrationEnv) {
	createdAt := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	seededAt := createdAt.Add(time.Hour)
	e.createLegacyHistory(legacyRecord{"1create", createdAt}, legacyRecord{"2seed", seededAt})
	e.exec(createUsersSQL)
	e.exec(insertUser(1, "alice"))
	e.exec(insertUser(2, "bob"))

	migs := lifecycleMigrations(e)
	require.NoError(t, e.up(migs...)) // Replaying 1create would fail since the table exists.
	e.requireHistory("1create", "2seed", "10rename")
	e.requireUsers(map[int]string{1: "alice", 2: "carol"})
	for _, m := range e.status() {
		switch m.ID {
		case "1create":
			requireTimeNear(t, createdAt, m.AppliedAt)
		case "2seed":
			requireTimeNear(t, seededAt, m.AppliedAt)
		}
	}

	require.NoError(t, e.down(migs...))
	e.requireHistory()
	require.False(t, e.tableExists("{users}"))
}

func testLegacyNullAppliedAt(t *gotesting.T, e *integrationEnv) {
	e.createLegacyHistory(legacyRecord{id: "1create"})
	e.exec(createUsersSQL)

	status := e.status()
	require.Len(t, status, 1)
	require.Equal(t, "1create", status[0].ID)
	require.True(t, status[0].AppliedAt.IsZero())

	require.NoError(t, e.up(lifecycleMigrations(e)...))
	e.requireHistory("1create", "2seed", "10rename")
	e.requireUsers(map[int]string{1: "alice", 2: "carol"})
}

// testLegacyUnknownAppliedID checks that an applied migration missing from the list prevents running any SQL.
func testLegacyUnknownAppliedID(t *gotesting.T, e *integrationEnv) {
	now := time.Now().UTC()
	e.createLegacyHistory(legacyRecord{"1create", now}, legacyRecord{"99unknown", now})
	e.exec(createUsersSQL)
	migs := lifecycleMigrations(e)

	require.ErrorContains(t, e.up(migs...), "99unknown")
	e.requireUsers(map[int]string{}) // 2seed did not run.

	require.ErrorContains(t, e.down(migs...), "99unknown")
	require.True(t, e.tableExists("{users}")) // 1create was not rolled back.
	e.requireHistory("1create", "99unknown")
}

// testLegacyCatchUpGap characterizes behavior inherited from sql-migrate: a migration missing in history but ordered
// before the last applied one is applied first in both directions, and the limit does not count it.
func testLegacyCatchUpGap(t *gotesting.T, e *integrationEnv) {
	now := time.Now().UTC()
	e.createLegacyHistory(legacyRecord{"1create", now}, legacyRecord{"3c", now})
	e.exec(createUsersSQL)
	e.exec(insertUser(3, "c"))
	migs := []migrate.Migration{
		e.mig("10j", []string{insertUser(10, "j")}, []string{"DELETE FROM {users} WHERE id = 10"}),
		e.mig("1create", []string{createUsersSQL}, []string{"DROP TABLE {users}"}),
		e.mig("2b", []string{insertUser(2, "b")}, []string{"DELETE FROM {users} WHERE id = 2"}),
		e.mig("3c", []string{insertUser(3, "c")}, []string{"DELETE FROM {users} WHERE id = 3"}),
	}

	require.NoError(t, e.mm.RunLimit(migs, migrate.MigrationsDirectionUp, 1))
	e.requireHistory("1create", "2b", "3c", "10j")
	e.requireUsers(map[int]string{2: "b", 3: "c", 10: "j"})

	// Recreate the gap.
	e.exec("DELETE FROM {users} WHERE id = 2")
	e.exec("DELETE FROM {meta} WHERE id = '2b'")

	// On Down the catch-up step runs its Up SQL but, as in sql-migrate v1.0.0, deletes its history row instead of
	// inserting it, so 2b stays missing from the history although its Up SQL was applied.
	require.NoError(t, e.mm.RunLimit(migs, migrate.MigrationsDirectionDown, 1))
	e.requireHistory("1create", "3c")
	e.requireUsers(map[int]string{2: "b", 3: "c"})
}
