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

	"github.com/acronis/go-dbkit/migrate"
)

// History edge cases. They write history rows directly into a metadata table created by the current package;
// upgrade provenance is covered by testUpgradeFromArtifact.
func historyEdgeScenarios() []integrationScenario {
	return []integrationScenario{
		{"HistoryNullAppliedAt", testHistoryNullAppliedAt},
		{"HistoryUnknownAppliedID", testHistoryUnknownAppliedID},
		{"HistoryCatchUpUp", testHistoryCatchUpUp},
		{"HistoryCatchUpDown", testHistoryCatchUpDown},
	}
}

// insertHistory writes a history row; a zero time is stored as NULL. The metadata table is created first.
func (e *integrationEnv) insertHistory(id string, appliedAt time.Time) {
	e.t.Helper()
	e.status()
	var value any
	if !appliedAt.IsZero() {
		value = appliedAt
	}
	e.exec(fmt.Sprintf("INSERT INTO {meta} (id, applied_at) VALUES (%s, %s)", e.b.placeholder(1), e.b.placeholder(2)), id, value)
}

// requireAppliedAt checks that the history row keeps the given timestamp.
func (e *integrationEnv) requireAppliedAt(id string, want time.Time) {
	e.t.Helper()
	for _, m := range e.status() {
		if m.ID == id {
			require.True(e.t, want.Equal(m.AppliedAt), "applied_at of %s: want %v, got %v", id, want, m.AppliedAt)
			return
		}
	}
	e.t.Fatalf("migration %s is not applied", id)
}

func testHistoryNullAppliedAt(t *gotesting.T, e *integrationEnv) {
	e.insertHistory("1create", time.Time{})
	e.exec(createUsersSQL)

	status := e.status()
	require.Len(t, status, 1)
	require.Equal(t, "1create", status[0].ID)
	require.True(t, status[0].AppliedAt.IsZero())

	require.NoError(t, e.up(lifecycleMigrations(e)...))
	e.requireHistory("1create", "2seed", "10rename")
	e.requireUsers(map[int]string{1: "alice", 2: "carol"})
	e.requireAppliedAt("1create", time.Time{})
}

// testHistoryUnknownAppliedID checks that an applied migration missing from the list prevents running any SQL.
func testHistoryUnknownAppliedID(t *gotesting.T, e *integrationEnv) {
	now := time.Now().UTC().Truncate(time.Second)
	e.insertHistory("1create", now)
	e.insertHistory("99unknown", now)
	e.exec(createUsersSQL)
	migs := lifecycleMigrations(e)

	require.ErrorContains(t, e.up(migs...), "99unknown")
	e.requireUsers(map[int]string{}) // 2seed did not run.

	require.ErrorContains(t, e.down(migs...), "99unknown")
	require.True(t, e.tableExists("{users}")) // 1create was not rolled back.
	e.requireHistory("1create", "99unknown")
}

// Catch-up characterization, inherited from sql-migrate v1.0.0: migrations missing from the history but ordered before
// the last applied one ("gaps") run their up SQL first, in both directions, and the limit does not count them.
// On down a gap is not recorded in the history, and the down plan walks all migrations ordered before the last applied
// one, including gaps. Every migration appends a marker row whose id is the next sequence number, so the markers
// show the execution order.

var catchUpAppliedAt = time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)

func catchUpMigrations(e *integrationEnv) []migrate.Migration {
	ids := []string{"7g", "1a", "2b", "3c", "4d", "5e", "6f"} // Unsorted on purpose.
	migs := make([]migrate.Migration, 0, len(ids))
	for _, id := range ids {
		migs = append(migs, e.mig(id, []string{markerSQL(id + "-up")}, []string{markerSQL(id + "-down")}))
	}
	return migs
}

func markerSQL(marker string) string {
	return fmt.Sprintf("INSERT INTO {items} (id, note) SELECT COALESCE(MAX(id), 0) + 1, '%s' FROM {items}", marker)
}

// resetCatchUp creates the marker table and the history 1a, 3c, 5e: gaps 2b and 4d, pending 6f and 7g.
func (e *integrationEnv) resetCatchUp() {
	e.t.Helper()
	e.exec("DROP TABLE IF EXISTS {items}")
	e.exec("CREATE TABLE {items} (id INT NOT NULL PRIMARY KEY, note VARCHAR(100) NOT NULL)")
	e.status()
	e.exec("DELETE FROM {meta}")
	for _, id := range []string{"1a", "3c", "5e"} {
		e.insertHistory(id, catchUpAppliedAt)
	}
}

// markers returns marker rows in execution order.
func (e *integrationEnv) markers() []string {
	e.t.Helper()
	rows, err := e.db.Query(e.q("SELECT note FROM {items} ORDER BY id"))
	require.NoError(e.t, err)
	defer func() { require.NoError(e.t, rows.Close()) }()
	res := []string{}
	for rows.Next() {
		var note string
		require.NoError(e.t, rows.Scan(&note))
		res = append(res, note)
	}
	require.NoError(e.t, rows.Err())
	return res
}

func (e *integrationEnv) requireRetainedAppliedAt(ids ...string) {
	e.t.Helper()
	for _, id := range ids {
		e.requireAppliedAt(id, catchUpAppliedAt)
	}
}

func testHistoryCatchUpUp(t *gotesting.T, e *integrationEnv) {
	migs := catchUpMigrations(e)

	e.resetCatchUp()
	require.NoError(t, e.mm.RunLimit(migs, migrate.MigrationsDirectionUp, 1))
	require.Equal(t, []string{"2b-up", "4d-up", "6f-up"}, e.markers()) // Gaps first; the limit counts 6f only.
	e.requireHistory("1a", "2b", "3c", "4d", "5e", "6f")
	e.requireRetainedAppliedAt("1a", "3c", "5e")

	e.resetCatchUp()
	require.NoError(t, e.mm.RunLimit(migs, migrate.MigrationsDirectionUp, 0))
	require.Equal(t, []string{"2b-up", "4d-up", "6f-up", "7g-up"}, e.markers())
	e.requireHistory("1a", "2b", "3c", "4d", "5e", "6f", "7g")
	e.requireRetainedAppliedAt("1a", "3c", "5e")
}

func testHistoryCatchUpDown(t *gotesting.T, e *integrationEnv) {
	migs := catchUpMigrations(e)

	e.resetCatchUp()
	require.NoError(t, e.mm.RunLimit(migs, migrate.MigrationsDirectionDown, 1))
	// Gap up SQL runs but the gaps are not recorded; the limit counts 5e only; pending 6f and 7g are untouched.
	require.Equal(t, []string{"2b-up", "4d-up", "5e-down"}, e.markers())
	e.requireHistory("1a", "3c")
	e.requireRetainedAppliedAt("1a", "3c")

	// A repeated down reaches the gap 2b: its up SQL runs again as catch-up, then its down SQL after 3c.
	require.NoError(t, e.mm.RunLimit(migs, migrate.MigrationsDirectionDown, 2))
	require.Equal(t, []string{"2b-up", "4d-up", "5e-down", "2b-up", "3c-down", "2b-down"}, e.markers())
	e.requireHistory("1a")
	e.requireRetainedAppliedAt("1a")

	e.resetCatchUp()
	require.NoError(t, e.mm.RunLimit(migs, migrate.MigrationsDirectionDown, 0))
	require.Equal(t, []string{"2b-up", "4d-up", "5e-down", "4d-down", "3c-down", "2b-down", "1a-down"}, e.markers())
	e.requireHistory()
}
