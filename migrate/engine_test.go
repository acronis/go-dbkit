/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package migrate

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/acronis/go-appkit/log/logtest"
	"github.com/stretchr/testify/require"

	"github.com/acronis/go-dbkit"
)

func rawMigrations(ids ...string) []*RawMigration {
	res := make([]*RawMigration, 0, len(ids))
	for _, id := range ids {
		res = append(res, &RawMigration{ID: id, UpSQL: []string{"up " + id}, DownSQL: []string{"down " + id}})
	}
	return res
}

func planIDs(plan []plannedMigration) []string {
	ids := make([]string, 0, len(plan))
	for _, p := range plan {
		ids = append(ids, p.id)
	}
	return ids
}

func TestMigrationIDLess(t *testing.T) {
	require.True(t, migrationIDLess("2_a", "10_a"))  // Numeric prefixes compare as numbers.
	require.False(t, migrationIDLess("10_a", "2_a")) //
	require.True(t, migrationIDLess("5", "abc"))     // Numeric before non-numeric.
	require.False(t, migrationIDLess("abc", "5"))    //
	require.True(t, migrationIDLess("abc", "abd"))   // Lexicographic otherwise.
	require.True(t, migrationIDLess("01_a", "1_b"))  // Equal numbers fall back to string comparison.
	require.True(t, migrationIDLess("99999999999999999999_a", "99999999999999999999_b"))
}

func TestPlanMigrations(t *testing.T) {
	tests := []struct {
		name      string
		available []string
		applied   []string
		direction MigrationsDirection
		limit     int
		want      []string
		wantErr   string
	}{
		{"up fresh", []string{"3", "1", "2"}, nil, MigrationsDirectionUp, 0, []string{"1", "2", "3"}, ""},
		{"up partial", []string{"1", "2", "3"}, []string{"1"}, MigrationsDirectionUp, 0, []string{"2", "3"}, ""},
		{"up limit", []string{"1", "2", "3"}, nil, MigrationsDirectionUp, 2, []string{"1", "2"}, ""},
		{"up all applied", []string{"1", "2"}, []string{"1", "2"}, MigrationsDirectionUp, 0, []string{}, ""},
		{"down all", []string{"1", "2", "3"}, []string{"1", "2", "3"}, MigrationsDirectionDown, 0, []string{"3", "2", "1"}, ""},
		{"down limit", []string{"1", "2", "3"}, []string{"1", "2", "3"}, MigrationsDirectionDown, 1, []string{"3"}, ""},
		{"down fresh", []string{"1", "2"}, nil, MigrationsDirectionDown, 0, []string{}, ""},
		{"up catch-up gap", []string{"1", "2", "3"}, []string{"1", "3"}, MigrationsDirectionUp, 0, []string{"2"}, ""},
		// Characterization: catch-up migrations are planned before the requested direction and are not limited.
		{"down catch-up gap", []string{"1", "2", "3"}, []string{"1", "3"}, MigrationsDirectionDown, 1, []string{"2", "3"}, ""},
		{"up catch-up and limit", []string{"1", "2", "3", "4"}, []string{"1", "3"}, MigrationsDirectionUp, 1, []string{"2", "4"}, ""},
		{"numeric order", []string{"10_b", "2_a"}, []string{"2_a"}, MigrationsDirectionUp, 0, []string{"10_b"}, ""},
		{"unknown applied", []string{"1"}, []string{"1", "2"}, MigrationsDirectionUp, 0, nil,
			"unable to create migration plan because of 2: unknown migration in database"},
		{"unknown direction", []string{"1"}, nil, "sideways", 0, nil, `unknown direction "sideways"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := planMigrations(rawMigrations(tt.available...), tt.applied, tt.direction, tt.limit)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, planIDs(plan))
		})
	}
}

func TestPlanMigrations_QueriesAndTxFlags(t *testing.T) {
	migs := []*RawMigration{{
		ID: "1", UpSQL: []string{"u"}, DownSQL: []string{"d"}, DisableTransactionUp: true,
	}}
	up, err := planMigrations(migs, nil, MigrationsDirectionUp, 0)
	require.NoError(t, err)
	require.Equal(t, []plannedMigration{{id: "1", queries: []string{"u"}, disableTx: true}}, up)

	down, err := planMigrations(migs, []string{"1"}, MigrationsDirectionDown, 0)
	require.NoError(t, err)
	require.Equal(t, []plannedMigration{{id: "1", queries: []string{"d"}, disableTx: false}}, down)
}

func newSQLiteManager(t *testing.T, db *sql.DB) *MigrationsManager {
	t.Helper()
	mm, err := NewMigrationsManager(db, dbkit.DialectSQLite, logtest.NewLogger())
	require.NoError(t, err)
	return mm
}

func countRows(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&n))
	return n
}

func TestMigrationsManager_failedMigrationRollsBackMetadata(t *testing.T) {
	db := openTestSQLite(t)
	mm := newSQLiteManager(t, db)

	migs := []Migration{
		NewCustomMigration("0001", []string{"CREATE TABLE t (id INT)"}, []string{"DROP TABLE t"}, nil, nil),
		NewCustomMigration("0002",
			[]string{"INSERT INTO t VALUES (1)", "INSERT INTO missing VALUES (1)"}, []string{"DELETE FROM t"}, nil, nil),
	}
	err := mm.Run(migs, MigrationsDirectionUp)
	require.ErrorContains(t, err, "no such table: missing")
	require.ErrorContains(t, err, " handling 0002")

	status, err := mm.Status()
	require.NoError(t, err)
	require.Len(t, status.AppliedMigrations, 1)
	require.Equal(t, "0001", status.AppliedMigrations[0].ID)
	require.Equal(t, 0, countRows(t, db, "t")) // Statements of the failed migration are rolled back.
}

func TestMigrationsManager_failedDownKeepsMetadata(t *testing.T) {
	db := openTestSQLite(t)
	mm := newSQLiteManager(t, db)

	migs := []Migration{NewCustomMigration("0001", []string{"CREATE TABLE t (id INT)"}, []string{"DROP TABLE missing"}, nil, nil)}
	require.NoError(t, mm.Run(migs, MigrationsDirectionUp))
	require.Error(t, mm.Run(migs, MigrationsDirectionDown))

	status, err := mm.Status()
	require.NoError(t, err)
	require.Len(t, status.AppliedMigrations, 1)
}

type txDisabledMigration struct {
	*CustomMigration
}

func (txDisabledMigration) DisableTx() bool { return true }

func TestMigrationsManager_disabledTxRecordsMetadataAfterSuccess(t *testing.T) {
	db := openTestSQLite(t)
	mm := newSQLiteManager(t, db)

	failing := txDisabledMigration{NewCustomMigration("0001",
		[]string{"CREATE TABLE t (id INT)", "INSERT INTO missing VALUES (1)"}, []string{"DROP TABLE t"}, nil, nil)}
	require.Error(t, mm.Run([]Migration{failing}, MigrationsDirectionUp))

	// Applied statements stay, metadata isn't written.
	require.Equal(t, 0, countRows(t, db, "t"))
	status, err := mm.Status()
	require.NoError(t, err)
	require.Empty(t, status.AppliedMigrations)

	ok := txDisabledMigration{NewCustomMigration("0002", []string{"INSERT INTO t VALUES (1)"}, []string{"DELETE FROM t"}, nil, nil)}
	_, err = db.Exec("INSERT INTO migrations (id, applied_at) VALUES ('0001', NULL)")
	require.NoError(t, err)
	require.NoError(t, mm.Run([]Migration{failing.CustomMigration, ok}, MigrationsDirectionUp))
	status, err = mm.Status()
	require.NoError(t, err)
	require.Len(t, status.AppliedMigrations, 2)
}

func TestMigrationsManager_legacyMetadataTable(t *testing.T) {
	db := openTestSQLite(t)

	// Table in the shape created by the previous implementation, with a recorded history.
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS "migrations" ("id" varchar(255) not null primary key, "applied_at" datetime)`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE users (id INT)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO migrations (id, applied_at) VALUES ('0001', '2024-01-02 03:04:05+00:00')`)
	require.NoError(t, err)

	mm := newSQLiteManager(t, db)
	migs := []Migration{
		NewCustomMigration("0001", []string{"CREATE TABLE users (id INT)"}, []string{"DROP TABLE users"}, nil, nil), // Would fail if replayed.
		NewCustomMigration("0002", []string{"INSERT INTO users VALUES (1)"}, []string{"DELETE FROM users"}, nil, nil),
	}
	require.NoError(t, mm.Run(migs, MigrationsDirectionUp))

	status, err := mm.Status()
	require.NoError(t, err)
	require.Len(t, status.AppliedMigrations, 2)
	require.Equal(t, "0001", status.AppliedMigrations[0].ID)
	require.Equal(t, 2024, status.AppliedMigrations[0].AppliedAt.Year())
	require.Equal(t, 1, countRows(t, db, "users"))

	require.NoError(t, mm.Run(migs, MigrationsDirectionDown))
	status, err = mm.Status()
	require.NoError(t, err)
	require.Empty(t, status.AppliedMigrations)
}

func TestMigrationsManager_unknownDialectAndDirection(t *testing.T) {
	db := openTestSQLite(t)
	mm, err := NewMigrationsManager(db, "oracle", logtest.NewLogger())
	require.NoError(t, err)
	migs := []Migration{NewCustomMigration("0001", []string{"SELECT 1"}, nil, nil, nil)}
	require.EqualError(t, mm.Run(migs, MigrationsDirectionUp), "unknown dialect: oracle")
	_, err = mm.Status()
	require.ErrorContains(t, err, "unknown dialect: oracle")

	mm = newSQLiteManager(t, db)
	require.EqualError(t, mm.Run(migs, "sideways"), `unknown direction "sideways"`)
}
