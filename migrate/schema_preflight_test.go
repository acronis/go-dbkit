/*
Copyright © 2026 Acronis International GmbH.

Released under MIT license.
*/

package migrate

import (
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/acronis/go-appkit/log/logtest"
	"github.com/stretchr/testify/require"

	"github.com/acronis/go-dbkit"
)

func newMockManager(t *testing.T) (*MigrationsManager, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	mm, err := NewMigrationsManager(db, dbkit.DialectSQLite, logtest.NewLogger())
	require.NoError(t, err)
	return mm, mock
}

// expectPreflight expects metadata table creation and SQLite schema inspection. Extra columns are
// (name, declared type, not null) triples.
func expectPreflight(mock sqlmock.Sqlmock, extra ...[]driver.Value) {
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "migrations"`).WillReturnResult(sqlmock.NewResult(0, 0))
	cols := sqlmock.NewRows([]string{"name", "type", "notnull", "dflt_value", "pk", "hidden"}).
		AddRow("id", "VARCHAR(255)", true, nil, 1, 0).
		AddRow("applied_at", "DATETIME", false, nil, 0, 0)
	for _, c := range extra {
		cols.AddRow(c[0], c[1], c[2], nil, 0, 0)
	}
	mock.ExpectQuery(`pragma_table_xinfo`).WithArgs("migrations").WillReturnRows(cols)
	mock.ExpectQuery(`pragma_index_list`).WithArgs("migrations").
		WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("sqlite_autoindex_migrations_1"))
	mock.ExpectQuery(`pragma_index_xinfo`).WithArgs("sqlite_autoindex_migrations_1").
		WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("id"))
}

func expectHistory(mock sqlmock.Sqlmock, ids ...string) {
	rows := sqlmock.NewRows([]string{"id", "applied_at"})
	for _, id := range ids {
		rows.AddRow(id, nil)
	}
	mock.ExpectQuery(`SELECT id, applied_at FROM "migrations"`).WillReturnRows(rows)
}

var requiredExtraColumn = []driver.Value{"tenant_id", "INT", true}

// preflightMigrations cover the default transaction, TxDisabler and raw transaction opt-out in both directions.
func preflightMigrations() map[string][]Migration {
	up := []string{"UPDATE users SET name = 'changed'"}
	down := []string{"DELETE FROM users"}
	return map[string][]Migration{
		"transaction":  {NewCustomMigration("0001", up, down, nil, nil)},
		"TxDisabler":   {&txDisabledMigration{NewCustomMigration("0001", up, down, nil, nil)}},
		"raw opt-out":  {&rawFlagsMigration{raw: RawMigration{ID: "0001", UpSQL: up, DownSQL: down, DisableTransactionUp: true, DisableTransactionDown: true}}},
		"raw mixed tx": {&rawFlagsMigration{raw: RawMigration{ID: "0001", UpSQL: up, DownSQL: down, DisableTransactionDown: true}}},
	}
}

type rawFlagsMigration struct {
	NullMigration
	raw RawMigration
}

func (m *rawFlagsMigration) ID() string { return m.raw.ID }

func (m *rawFlagsMigration) RawMigration(Migration) (*RawMigration, error) {
	raw := m.raw
	return &raw, nil
}

// TestMigrationsManager_preflightOrder checks the order create -> inspect -> list -> transaction -> SQL.
func TestMigrationsManager_preflightOrder(t *testing.T) {
	mm, mock := newMockManager(t)
	expectPreflight(mock)
	expectHistory(mock)
	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE users`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO "migrations"`).WithArgs("0001", sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	require.NoError(t, mm.Run(preflightMigrations()["transaction"], MigrationsDirectionUp))

	expectPreflight(mock)
	expectHistory(mock, "0001")
	_, err := mm.Status()
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestMigrationsManager_preflightRejectsIncompatibleTable checks that Run and Status stop after inspection: no history
// read, no transaction and no migration SQL, for every transaction mode and direction.
func TestMigrationsManager_preflightRejectsIncompatibleTable(t *testing.T) {
	for name, migs := range preflightMigrations() {
		for _, direction := range []MigrationsDirection{MigrationsDirectionUp, MigrationsDirectionDown} {
			t.Run(name+"/"+string(direction), func(t *testing.T) {
				mm, mock := newMockManager(t)
				expectPreflight(mock, requiredExtraColumn)
				err := mm.Run(migs, direction)
				var incompatible *incompatibleSchemaError
				require.ErrorAs(t, err, &incompatible)
				require.EqualError(t, err, `migration history table "migrations" (dialect sqlite3) is incompatible: `+
					`column "tenant_id" is NOT NULL without a default; `+
					`configure MigrationsManagerOpts.TableName to use a separate migration history table`)
				require.NoError(t, mock.ExpectationsWereMet())
			})
		}
	}

	t.Run("Status", func(t *testing.T) {
		mm, mock := newMockManager(t)
		expectPreflight(mock, requiredExtraColumn)
		_, err := mm.Status()
		var incompatible *incompatibleSchemaError
		require.ErrorAs(t, err, &incompatible)
		require.ErrorContains(t, err, `get applied migrations: migration history table "migrations"`)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestMigrationsManager_preflightInspectionError(t *testing.T) {
	errCatalog := errors.New("catalog failure")
	run := map[string]func(mm *MigrationsManager) error{
		"up": func(mm *MigrationsManager) error {
			return mm.Run(preflightMigrations()["TxDisabler"], MigrationsDirectionUp)
		},
		"down": func(mm *MigrationsManager) error {
			return mm.Run(preflightMigrations()["TxDisabler"], MigrationsDirectionDown)
		},
		"Status": func(mm *MigrationsManager) error { _, err := mm.Status(); return err },
	}
	for name, call := range run {
		t.Run(name, func(t *testing.T) {
			mm, mock := newMockManager(t)
			mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "migrations"`).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(`pragma_table_xinfo`).WillReturnError(errCatalog)
			err := call(mm)
			require.ErrorIs(t, err, errCatalog)
			require.ErrorContains(t, err, `inspect migration history table "migrations"`)
			var incompatible *incompatibleSchemaError
			require.False(t, errors.As(err, &incompatible), "an inspection failure is not an incompatibility")
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestMigrationsManager_preflightNotCached changes the schema between calls of one manager.
func TestMigrationsManager_preflightNotCached(t *testing.T) {
	db := openTestSQLite(t)
	mm := newSQLiteManager(t, db)
	_, err := db.Exec("CREATE TABLE users (id INT, name TEXT)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO users VALUES (1, 'alice')")
	require.NoError(t, err)
	migs := preflightMigrations()["transaction"]

	_, err = mm.Status()
	require.NoError(t, err)

	// Recreate the history table with a required extra column, keeping the history.
	for _, stmt := range []string{
		"ALTER TABLE migrations RENAME TO migrations_old",
		"CREATE TABLE migrations (id VARCHAR(255) NOT NULL PRIMARY KEY, applied_at DATETIME, tenant_id INT NOT NULL)",
		"DROP TABLE migrations_old",
	} {
		_, err = db.Exec(stmt)
		require.NoError(t, err)
	}
	_, err = mm.Status()
	require.ErrorContains(t, err, `column "tenant_id" is NOT NULL without a default`)
	require.ErrorContains(t, mm.Run(migs, MigrationsDirectionUp), `column "tenant_id" is NOT NULL without a default`)
	var name string
	require.NoError(t, db.QueryRow("SELECT name FROM users WHERE id = 1").Scan(&name))
	require.Equal(t, "alice", name) // The migration did not run.

	// A manager with a separate history table proceeds; the rejected table stays as it is.
	other, err := NewMigrationsManagerWithOpts(db, dbkit.DialectSQLite, logtest.NewLogger(), MigrationsManagerOpts{TableName: "migrations_v2"})
	require.NoError(t, err)
	require.NoError(t, other.Run(migs, MigrationsDirectionUp))
	require.NoError(t, db.QueryRow("SELECT name FROM users WHERE id = 1").Scan(&name))
	require.Equal(t, "changed", name)
	var rows int
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM migrations").Scan(&rows))
	require.Zero(t, rows)

	// Restoring a compatible table makes the original manager work again.
	_, err = db.Exec("DROP TABLE migrations")
	require.NoError(t, err)
	_, err = mm.Status()
	require.NoError(t, err)
}
